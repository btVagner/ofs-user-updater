import os


DEFAULT_OFS_ROOT_RESOURCE_ID = "02"


def get_ofs_root_resource_id() -> str:
    """Retorna o root único usado por sincronizações hierárquicas do OFS.

    OFS_ROOT_RESOURCE_ID é a configuração preferencial. Para manter
    compatibilidade com o dashboard atual, DASHBOARD_OFS_RESOURCES é aceito
    como fallback somente quando contém um único recurso.
    """
    explicit_root = (os.getenv("OFS_ROOT_RESOURCE_ID") or "").strip()
    if explicit_root:
        return explicit_root

    dashboard_resources = (os.getenv("DASHBOARD_OFS_RESOURCES") or "").strip()
    if dashboard_resources and "," not in dashboard_resources and ";" not in dashboard_resources:
        return dashboard_resources

    return DEFAULT_OFS_ROOT_RESOURCE_ID
