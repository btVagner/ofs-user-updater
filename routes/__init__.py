from .auth_routes import init_app as init_auth_routes
from .logs_routes import init_app as init_logs_routes
from .adapter_routes import init_app as init_adapter_routes
from .ofs_user_management_routes import init_app as init_ofs_user_management_routes
from .ofs_activities_errors_routes import init_app as init_ofs_activities_errors_routes
from .atividades_notdone_routes import init_app as init_atividades_notdone_routes
from .sap_acompanhamento_critica_routes import init_app as init_sap_acompanhamento_critica_routes
from .ofs_atividades_base_routes import init_app as init_ofs_atividades_base_routes
from .perfis_usuarios_routes import init_app as init_perfis_usuarios_routes
from .ofs_reprocessing_routes import init_app as init_ofs_reprocessing_routes
from .ddc_mensageria_routes import init_app as init_ddc_mensageria_routes
from .ofs_erros_tratativas_dashboards_routes import init_app as init_ofs_erros_tratativas_dashboards_routes


def register_routes(app):
    init_auth_routes(app)
    init_logs_routes(app)
    init_adapter_routes(app)

    init_ofs_user_management_routes(app)
    init_ofs_activities_errors_routes(app)
    init_atividades_notdone_routes(app)
    init_sap_acompanhamento_critica_routes(app)
    init_ofs_atividades_base_routes(app)
    init_perfis_usuarios_routes(app)
    init_ofs_reprocessing_routes(app)
    init_ddc_mensageria_routes(app)
    init_ofs_erros_tratativas_dashboards_routes(app)