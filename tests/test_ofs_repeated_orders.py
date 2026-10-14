from contextlib import contextmanager
from datetime import date, datetime, timedelta

import pytest
from services.ofs_repeated_orders_service import (
    FIELDS, RepeatedOrdersError, analyze, apply_status_policy, collect, collect_scope, reconcile_duplicates,
    remaining_seconds, run_worker_once, normalize_state, STATE_NAMES,
)

DAY = date(2026, 10, 14)


def activity(identifier, appt=None, contract='0000123', kind='INST', origin='NG', day=DAY, **kw):
    return dict(activityId=str(identifier), apptNumber=appt if appt is not None else str(identifier),
                customerNumber=contract, activityType=kind, XA_ORG_SYS=origin, date=day.isoformat(),
                status='pending', resourceId='T1', recordType='regular', **kw)


@pytest.mark.parametrize('types,severity', [(['INST','MAN'], 'yellow'), (['INST','INST'], 'red'),
    (['INST','MAN','VIS'], 'purple'), (['INST'] * 4, 'purple')])
def test_severity(types, severity):
    result = analyze([activity(i, kind=kind) for i, kind in enumerate(types)], DAY)
    assert result['contracts'][0]['severity'] == severity
    assert result['contracts'][0]['count'] == len(types)
    assert result['contracts'][0]['contract'] == '0000123'


def test_ng_prefix_not_suffix_and_linked_multiple_types():
    rows = [activity(1, '0001-A'), activity(2, '0001-B', kind='MAN'), activity(3, '0002-B')]
    result = analyze(rows, DAY)
    assert result['contracts'][0]['count'] == 2
    assert result['contracts'][0]['severity'] == 'yellow'
    assert len(result['contracts'][0]['orders'][0]['activities']) == 2
    assert analyze(rows[:2], DAY)['contracts'] == []


def test_non_ng_full_number_and_origin_namespace():
    rows = [activity(1, '0001-A', origin='Adapter'), activity(2, '0001-B', origin='Adapter'),
            activity(3, '0001-A', origin='NG')]
    assert analyze(rows, DAY)['contracts'][0]['count'] == 3


def test_deduplicates_activity_and_os_without_losing_details():
    a = activity(1, '123', origin='Adapter')
    result = analyze([a, a, activity(2, '123', origin='Adapter'), activity(3, '456', origin='Adapter')], DAY)
    assert result['contracts'][0]['count'] == 2
    assert result['diagnostics']['duplicate_activity'] == 1
    assert result['diagnostics']['duplicate_os'] == 1


def test_conflicting_activity_id_fails_closed():
    with pytest.raises(RepeatedOrdersError, match='conflitante'):
        analyze([activity(1), activity(1, kind='MAN')], DAY)


def test_window_inclusive_and_contracts_separate():
    result = analyze([activity(1), activity(2, day=DAY+timedelta(days=6)),
        activity(3, day=DAY+timedelta(days=7)), activity(4, day=DAY-timedelta(days=1)),
        activity(5, contract='other')], DAY)
    assert len(result['contracts']) == 1
    assert result['contracts'][0]['count'] == 2
    assert result['diagnostics']['outside_window'] == 2
    assert result['date_to'] == '2026-10-20'


def test_incomplete_rows_explicit_and_no_false_alerts():
    result = analyze([activity(1, contract=''), activity(2, appt=''), activity(3, appt='-invalid'),
                      activity(4, kind=''), {**activity(5), 'date': ''}, activity(6, kind='LUNCH')], DAY)
    assert result['contracts'] == []
    assert result['diagnostics'] == dict(missing_contract=1, missing_appt=2, missing_type=1,
        invalid_date=1, outside_window=0, non_os=1, withdrawal=0, outside_hierarchy=0,
        missing_resource=0, excluded_status=0, duplicate_activity=0, duplicate_os=0)
    assert result['diagnostic_samples']['missing_contract'][0]['activityId'] == '1'
    assert len(result['diagnostic_samples']['missing_appt']) == 2
    with pytest.raises(RepeatedOrdersError, match='activityId'):
        analyze([{**activity(1), 'activityId': ''}], DAY)


@pytest.mark.parametrize('status', ['pending','enroute','started',' STARTED '])
def test_allowed_statuses_and_normalization(status):
    result = analyze([activity(1, origin=' ng '), {**activity(2, kind='inst'), 'status':status}], DAY)
    assert result['contracts'][0]['severity'] == 'red'


@pytest.mark.parametrize('status', ['notdone','cancelled','completed','suspended','',None])
def test_other_statuses_cannot_create_an_alert(status):
    result = analyze([activity(1), {**activity(2),'status':status}], DAY)
    assert result['contracts'] == []
    assert result['diagnostics']['excluded_status'] == 1


def test_existing_snapshot_is_narrowed_and_reclassified_without_mutation():
    old = analyze([activity(1),activity(2),activity(3,kind='MAN')], DAY)
    old.pop('allowed_statuses')
    old['contracts'][0]['orders'][2]['activities'][0]['status'] = 'cancelled'
    new = apply_status_policy(old)
    assert old['contracts'][0]['count'] == 3 and old['summary']['purple'] == 1
    assert new['contracts'][0]['count'] == 2 and new['summary']['red'] == 1
    assert new['diagnostics']['excluded_status_from_previous_mesh'] == 1
    assert new['received'] == old['received'] and new['date_from'] == old['date_from']
    assert apply_status_policy(new) is new


def test_existing_ng_snapshot_types_use_only_remaining_active_activities():
    old = analyze([activity(1,'111-A'),activity(2,'111-B',kind='MAN'),activity(3,'222-A')],DAY)
    old.pop('allowed_statuses')
    old['contracts'][0]['orders'][0]['activities'][1]['status'] = 'notdone'
    new = apply_status_policy(old)
    assert new['contracts'][0]['types'] == ['INST']
    assert new['summary']['red'] == 1 and new['contracts'][0]['count'] == 2
    old['contracts'][0]['orders'][1]['activities'][0]['status'] = 'cancelled'
    assert apply_status_policy(old)['contracts'] == []


def test_current_version_status_is_reapplied_after_reconciliation():
    class CurrentAPI:
        def _request(self, *_args, **_kw): return {**activity(1),'status':'notdone'}
    rows,_ = reconcile_duplicates(CurrentAPI(), [activity(1),{**activity(1),'status':'started'},activity(2)])
    assert analyze(rows,DAY)['contracts'] == []


class API:
    def __init__(self, pages):
        self.pages, self.calls = iter(pages), []

    def _request(self, method, path, params=None):
        if path == 'resources/02/descendants':
            return {'items':[{'resourceId':'T1'}], 'totalResults':1}
        assert (method, path) == ('GET', 'activities')
        self.calls.append(params)
        payload = next(self.pages)
        if isinstance(payload, Exception):
            raise payload
        return payload


def test_paging_hasmore_and_actual_count_even_short_pages():
    api = API([{'items':[activity(1)], 'hasMore':True}, {'items':[activity(2), activity(3)], 'hasMore':True},
               {'items':[activity(4)], 'hasMore':False}])
    progress = []
    assert len(collect(api, DAY, '02', lambda *args: progress.append(args), limit=1000)) == 4
    assert [call['offset'] for call in api.calls] == [0, 1, 3]
    assert progress == [(1,1),(2,3),(3,4)]
    assert set(api.calls[0]['fields'].split(',')) == set(FIELDS)
    assert api.calls[0]['dateTo'] == '2026-10-20'
    assert 'q' not in api.calls[0]


def test_false_or_absent_hasmore_ends_even_full_page():
    for page in ({'items':[activity(1)], 'hasMore':False}, {'items':[activity(1)]}):
        api = API([page])
        assert len(collect(api, DAY, '02', lambda *_: None, limit=1)) == 1
        assert len(api.calls) == 1


@pytest.mark.parametrize('page', [{'items':[], 'hasMore':True}, {'items':{}}, {},
    {'items':[None]}, {'items':[], 'hasMore':'true'}])
def test_malformed_page_fails(page):
    with pytest.raises(RepeatedOrdersError):
        collect(API([page]), DAY, '02', lambda *_: None)


def test_page_safety_cap():
    with pytest.raises(RepeatedOrdersError, match='páginas'):
        collect(API([{'items':[activity(1)], 'hasMore':True}]), DAY, '02', lambda *_: None, max_pages=1)


class Repository:
    def __init__(self):
        self.finished = []
        self.progresses = []

    @contextmanager
    def worker_lock(self):
        yield None, None

    def claim(self, *_):
        return 1

    def progress(self, *args):
        self.progresses.append(args[-2:])

    def finish(self, *_, **kw):
        self.finished.append(kw)


def test_late_page_failure_never_publishes_partial():
    repo = Repository()
    api = API([{'items':[activity(1)], 'hasMore':True}, RuntimeError('OFS unavailable')])
    assert run_worker_once(repo, lambda: api)
    assert repo.progresses == [(0,0),(1,1)]
    assert repo.finished == [{'error':'OFS unavailable'}]


def test_no_request_never_constructs_api():
    repo = Repository()
    repo.claim = lambda *_: None
    assert not run_worker_once(repo, lambda: pytest.fail('OFS should not be called'))


def test_interval_uses_actual_start_after_delayed_queue():
    now = datetime(2026, 10, 14, 12)
    gate = {'db_now': now, 'last_requested_at': now-timedelta(hours=1), 'last_started_at': now-timedelta(seconds=1)}
    assert remaining_seconds(gate) == 599
    gate['last_started_at'] = now-timedelta(seconds=600)
    assert remaining_seconds(gate) == 0


@pytest.mark.parametrize('kind', ['RET','RET_INST','RET-CLIENTE','Retirada','RETIRADAS_EQP'])
def test_withdrawals_cannot_create_alerts(kind):
    result = analyze([activity(1), activity(2, kind=kind)], DAY)
    assert result['contracts'] == []
    assert result['diagnostics']['withdrawal'] == 1


def test_hierarchy_filters_root_children_unknown_and_missing_resource():
    rows = [activity(1), activity(2), {**activity(3), 'resourceId':'outside'},
            {**activity(4), 'resourceId':''}, {**activity(5), 'resourceId':'02'}]
    result = analyze(rows, DAY, allowed_resources={'02','T1'})
    assert result['contracts'][0]['count'] == 3
    assert result['diagnostics']['outside_hierarchy'] == 1
    assert result['diagnostics']['missing_resource'] == 1
    assert result['diagnostic_samples']['outside_hierarchy'][0]['resourceId'] == 'outside'


def test_collection_cannot_override_root():
    with pytest.raises(RepeatedOrdersError, match='recurso 02'):
        collect(API([]), DAY, '03', lambda *_:None)


def test_scope_pagination_uses_actual_received_count():
    class ScopeAPI:
        def __init__(self): self.offsets = []
        def _request(self, method, path, params):
            assert path == 'resources/02/descendants' and params['fields'] == 'resourceId'
            self.offsets.append(params['offset'])
            return {'items':[{'resourceId':f'T{params["offset"]}'}], 'totalResults':2}
    api = ScopeAPI()
    assert collect_scope(api) == {'02','T0','T1'}
    assert api.offsets == [0,1]


@pytest.mark.parametrize('payload', [{'items':[], 'totalResults':1}, {'items':[None], 'totalResults':1},
                                   {'items':[], 'totalResults':'1'}])
def test_incomplete_scope_fails_closed(payload):
    class ScopeAPI:
        def _request(self, *_args, **_kw): return payload
    with pytest.raises(RepeatedOrdersError): collect_scope(ScopeAPI())


def test_scope_resources_without_external_id_do_not_enter_whitelist():
    class ScopeAPI:
        def _request(self, *_args, **_kw):
            return {'items':[{'resourceId':'T1'},{'resourceInternalId':999},{}], 'totalResults':3}
    metrics = {}
    assert collect_scope(ScopeAPI(), metrics=metrics) == {'02','T1'}
    assert metrics == {'received':3, 'missing_external_id':2, 'pages':1}


def test_reconciles_current_truth_once_then_reapplies_date_type_and_scope():
    class CurrentAPI:
        def __init__(self): self.calls = []
        def _request(self, method, path):
            self.calls.append(path)
            return {**activity(1, kind='RET'), 'resourceId':'outside'}
    api = CurrentAPI()
    first = activity(1)
    moved = {**first, 'status':'started', 'resourceId':'T2'}
    rows, stats = reconcile_duplicates(api, [first, moved, first])
    assert api.calls == ['activities/1']
    assert stats['duplicate_activity'] == 2 and stats['reconciled_activity'] == 1
    assert stats['reconciliation_samples'][0]['changed_fields'] == ['status','resourceId']
    assert analyze(rows, DAY, {'02','T1'})['contracts'] == []
    assert analyze(rows, DAY, {'02','T1'})['diagnostics']['withdrawal'] == 1


def test_exact_duplicates_make_no_detail_calls():
    class NeverAPI:
        def _request(self, *_args, **_kw): pytest.fail('Identical duplicates need no lookup')
    row = activity(1)
    rows, stats = reconcile_duplicates(NeverAPI(), [row,row])
    assert rows == [row] and stats['reconciled_activity'] == 0


def test_reconciliation_is_bounded_and_checks_returned_id():
    class WrongAPI:
        def _request(self, *_args, **_kw): return activity(2)
    rows = [activity(1), {**activity(1),'status':'started'}]
    with pytest.raises(RepeatedOrdersError, match='Muitas atividades'):
        reconcile_duplicates(WrongAPI(), rows, max_reconciliations=0)
    with pytest.raises(RepeatedOrdersError, match='activityId diferente'):
        reconcile_duplicates(WrongAPI(), rows)


def test_reconciliation_can_remove_a_rescheduled_order_from_window():
    class CurrentAPI:
        def _request(self, *_args, **_kw): return activity(1, day=DAY+timedelta(days=7))
    rows, _ = reconcile_duplicates(CurrentAPI(), [activity(1),{**activity(1),'status':'started'},activity(2)])
    result = analyze(rows, DAY, {'02','T1'})
    assert result['contracts'] == [] and result['diagnostics']['outside_window'] == 1


def test_worker_reconciliation_publishes_one_activity_and_ignores_root_override(monkeypatch):
    import services.ofs_repeated_orders_service as service
    monkeypatch.setattr(service, 'operational_today', lambda: DAY)
    monkeypatch.setenv('OFS_REPEATED_ORDERS_ROOT_ID', '99')
    monkeypatch.setenv('OFS_MONITOR_CASA_CLIENTE_ROOT_ID', '99')
    class WorkerAPI:
        def _request(self, method, path, params=None):
            if path == 'resources/02/descendants':
                return {'items':[{'resourceId':'T1'}], 'totalResults':1}
            if path == 'activities':
                assert params['resources'] == '02' and params['includeChildren'] == 'all'
                return {'items':[activity(1),{**activity(1),'status':'started'},activity(2,kind='MAN')], 'hasMore':False}
            assert path == 'activities/1'
            return {**activity(1),'status':'started'}
    repo = Repository()
    assert run_worker_once(repo, WorkerAPI)
    payload = repo.finished[0]['payload']
    assert payload['contracts'][0]['count'] == 2
    assert payload['contracts'][0]['severity'] == 'yellow'
    assert payload['diagnostics']['duplicate_activity'] == 1
    assert payload['diagnostics']['reconciled_activity'] == 1
    assert payload['root_resource_id'] == '02' and payload['received'] == 3


def test_failed_reconciliation_never_publishes_collected_pages(monkeypatch):
    import services.ofs_repeated_orders_service as service
    monkeypatch.setattr(service, 'operational_today', lambda: DAY)
    class WorkerAPI:
        def _request(self, method, path, params=None):
            if path == 'resources/02/descendants': return {'items':[{'resourceId':'T1'}], 'totalResults':1}
            if path == 'activities': return {'items':[activity(1),{**activity(1),'status':'started'}], 'hasMore':False}
            raise RuntimeError('detail lookup failed')
    repo = Repository()
    run_worker_once(repo, WorkerAPI)
    assert repo.finished == [{'error':'detail lookup failed'}]


def test_scope_change_aborts_without_using_a_partial_whitelist():
    class ScopeAPI:
        def _request(self, method, path, params):
            return {'items':[{'resourceId':str(params['offset'])}], 'totalResults':2 + params['offset']}
    with pytest.raises(RepeatedOrdersError, match='Hierarquia mudou'):
        collect_scope(ScopeAPI())


@pytest.mark.parametrize('code,name', STATE_NAMES.items())
def test_state_abbreviation_and_full_name_use_same_filter(code, name):
    assert normalize_state(code.lower()) == code
    assert normalize_state(name.lower()) == code


@pytest.mark.parametrize('value,expected', [('  São   Paulo ', 'SP'), ('Goiás','GO'),
    ('Rio Grande do Sul','RS'), ('SUL',''), ('JUIZ DE FORA',''), (None,''), ('invalid','')])
def test_state_normalization_does_not_invent_uf(value, expected):
    assert normalize_state(value) == expected


def test_regional_and_timeslot_belong_to_each_activity_and_survive_old_status_projection():
    rows = [activity(1,stateProvince='Minas Gerais',timeSlot='08:00-12:00'),
            activity(2,stateProvince='rs',timeSlot='13:00-18:00')]
    payload = analyze(rows + [rows[0]], DAY)
    details = [a for order in payload['contracts'][0]['orders'] for a in order['activities']]
    assert [(a['regional'],a['timeSlot']) for a in details] == [('MG','08:00-12:00'),('RS','13:00-18:00')]
    payload.pop('allowed_statuses')
    assert apply_status_policy(payload)['contracts'] == payload['contracts']


def test_worker_uses_state_on_os_without_regional_register(monkeypatch):
    import services.ofs_repeated_orders_service as service
    monkeypatch.setattr(service, 'operational_today', lambda: DAY)
    repo = Repository()
    run_worker_once(repo, lambda: API([{'items':[activity(1,stateProvince='SP'),activity(2,stateProvince='São Paulo')],'hasMore':False}]))
    payload = repo.finished[0]['payload']
    assert payload['regional_source'] == 'activity.stateProvince'
    assert {a['regional'] for o in payload['contracts'][0]['orders'] for a in o['activities']} == {'SP'}
