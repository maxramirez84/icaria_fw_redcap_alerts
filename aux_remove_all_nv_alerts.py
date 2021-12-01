from datetime import datetime
import redcap
import params
import alerts

for project_key in params.PROJECTS:
    project = redcap.Project(params.URL, params.PROJECTS[project_key])

    # Get all records for each ICARIA REDCap project
    print("[{}] Getting all records from {}...".format(datetime.now(), project_key))
    df = project.export_records(format='df')

    alerts.remove_nv_alerts(project, df, params.NV_ALERT)