from types import SimpleNamespace


def build_dag_parameters(
    conf: dict,
) -> tuple:
    project_id = conf.get("project_id", "default_project")
    tablas_dict = conf.get("tablas", {})
    params_dict = conf.get("params", {})
    flags_dict = conf.get("flags", {})

    # Construcción de rutas absolutas con backticks: `project.dataset.table`

    t_dict = {alias: f"`{project_id}.{ruta}`" for alias, ruta in tablas_dict.items()}

    return (
        t_dict,
        params_dict,
        flags_dict,
    )
