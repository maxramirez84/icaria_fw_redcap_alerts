#!/usr/bin/env python
""" Python script to setup alerts for ICARIA field workers. These alerts are for them to know if they have to do a
household visit after AZi/Pbo administration or a Non-Compliant visit. In the context of the ICARIA Clinical Trial, a
household visit is scheduled few days after the administration of the investigational product (azithromycin in this
case). Moreover, if study participants are not coming to the scheduled study visits, another household visit will be
scheduled to capture their status. This script is computing regularly which of the participants requires a household or
Non-Compliant visit. This requirement is saved into an eCRF variable in the Screening DCI. This variable will be setup
as part of the REDCap custom record label. Like this, field workers will see in a glance which participants they need to
visit at their households."""

from datetime import datetime
import redcap
import params
import alerts

__author__ = "Maximo Ramirez Robles"
__copyright__ = "Copyright 2021, ISGlobal Maternal, Child and Reproductive Health"
__credits__ = ["Maximo Ramirez Robles"]
__license__ = "MIT"
__version__ = "0.0.1"
__date__ = "20210323"
__maintainer__ = "Maximo Ramirez Robles"
__email__ = "maximo.ramirez@isglobal.org"
__status__ = "Dev"

if __name__ == '__main__':
    # Alerts system @ ICARIA TRIAL REDCap projects
    for project_key in params.TRIAL_PROJECTS:
        project = redcap.Project(params.URL, params.TRIAL_PROJECTS[project_key])

        # Get all records for each ICARIA REDCap project (TRIAL)
        print("[{}] Getting records from the ICARIA TRIAL REDCap projects:".format(datetime.now()))
        print("[{}] Getting all records from {}...".format(datetime.now(), project_key))
        df = project.export_records(format='df')

        # Custom status
        custom_status_ids = alerts.get_record_ids_with_custom_status(
            redcap_data=df,
            defined_alerts=params.TRIAL_DEFINED_ALERTS,
            fu_status_event=params.TRIAL_CHILD_FU_STATUS_EVENT
        )

        # Households to be visited
        if params.TBV_ALERT in params.TRIAL_DEFINED_ALERTS:
            alerts.set_tbv_alerts(
                redcap_project=project,
                redcap_project_df=df,
                tbv_alert=params.TBV_ALERT,
                tbv_alert_string=params.TBV_ALERT_STRING,
                redcap_date_format=params.REDCAP_DATE_FORMAT,
                alert_date_format=params.ALERT_DATE_FORMAT,
                choice_sep=params.CHOICE_SEP,
                code_sep=params.CODE_SEP,
                blocked_records=custom_status_ids,
                fu_status_event=params.TRIAL_CHILD_FU_STATUS_EVENT
            )

        # Non-compliant visits
        if params.NC_ALERT in params.TRIAL_DEFINED_ALERTS:
            # Update REDCap data as it has may been modified by set_tbv_alerts
            df = project.export_records(format='df')

            alerts.set_nc_alerts(
                redcap_project=project,
                redcap_project_df=df,
                nc_alert=params.NC_ALERT,
                nc_alert_string=params.NC_ALERT_STRING,
                choice_sep=params.CHOICE_SEP,
                code_sep=params.CODE_SEP,
                days_to_nc=params.DAYS_TO_NC,
                blocked_records=custom_status_ids,
                fu_status_event=params.TRIAL_CHILD_FU_STATUS_EVENT
            )

        # Next visit
        if params.NV_ALERT in params.TRIAL_DEFINED_ALERTS:
            # Update REDCap data as it has may been modified by set_nc_alerts
            df = project.export_records(format='df')

            alerts.set_nv_alerts(
                redcap_project=project,
                redcap_project_df=df,
                nv_alert=params.NV_ALERT,
                nv_alert_string=params.NV_ALERT_STRING,
                alert_date_format=params.ALERT_DATE_FORMAT,
                days_before=params.DAYS_BEFORE_NV,
                days_after=params.DAYS_AFTER_NV,
                blocked_records=custom_status_ids,
                fu_status_event=params.TRIAL_CHILD_FU_STATUS_EVENT
            )

        # End of Follow Up
        if params.END_FU_ALERT in params.TRIAL_DEFINED_ALERTS:
            # Update REDCap data as it has may been modified by set_nv_alerts
            df = project.export_records(format='df')

            alerts.set_end_fu_alerts(
                redcap_project=project,
                redcap_project_df=df,
                end_fu_alert=params.END_FU_ALERT,
                end_fu_alert_string=params.END_FU_ALERT_STRING,
                alert_date_format=params.ALERT_DATE_FORMAT,
                days_before=params.DAYS_BEFORE_END_FU,
                blocked_records=custom_status_ids,
                study="TRIAL",  # 18 months and end of follow up household visit
                fu_status_event=params.TRIAL_CHILD_FU_STATUS_EVENT,
                months=params.END_FU_TRIAL
            )

    # Alerts system @ ICARIA COHORT REDCap projects
    for project_key in params.COHORT_PROJECTS:
        project = redcap.Project(params.URL, params.COHORT_PROJECTS[project_key])

        # Get all records for each ICARIA REDCap project (COHORT)
        print("[{}] Getting records from the ICARIA COHORT REDCap projects:".format(datetime.now()))
        print("[{}] Getting all records from {}...".format(datetime.now(), project_key))
        fields = project.export_field_names()
        field_names = [field['export_field_name'] for field in fields]
        df = project.export_records(format='df', fields=field_names)

        # Custom status
        custom_status_ids = alerts.get_record_ids_with_custom_status(
            redcap_data=df,
            defined_alerts=params.COHORT_DEFINED_ALERTS,
            fu_status_event=params.COHORT_CHILD_FU_STATUS_EVENT
        )

        # End of Follow Up
        if params.END_FU_ALERT in params.COHORT_DEFINED_ALERTS:
            alerts.set_end_fu_alerts(
                redcap_project=project,
                redcap_project_df=df,
                end_fu_alert=params.END_FU_ALERT,
                end_fu_alert_string=params.END_FU_ALERT_STRING,
                alert_date_format=params.ALERT_DATE_FORMAT,
                days_before=params.DAYS_BEFORE_END_FU,
                blocked_records=custom_status_ids,
                study="COHORT",  # 15 months and end of follow up tests
                fu_status_event=params.COHORT_CHILD_FU_STATUS_EVENT,
                months=params.END_FU_COHORT
            )
