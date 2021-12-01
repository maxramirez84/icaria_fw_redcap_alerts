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
import tokens
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
    URL = tokens.URL
    PROJECTS = tokens.REDCAP_PROJECTS
    TBV_ALERT = "TBV"
    TBV_ALERT_STRING = TBV_ALERT + "@{community} AZi/Pbo@{last_azi_date}"
    CHOICE_SEP = " | "
    CODE_SEP = ", "
    REDCAP_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    ALERT_DATE_FORMAT = "%b %d"
    DAYS_TO_NC = 28  # Defined by PI as 4 weeks
    NC_ALERT = "NC"
    NC_ALERT_STRING = NC_ALERT + "@{community} ({weeks} weeks)"
    DAYS_BEFORE_NV = 7  # Defined by In-Country Technical Coordinator
    DAYS_AFTER_NV = DAYS_TO_NC  # Defined by In-Country Technical Coordinator
    NV_ALERT = "NEXT VISIT"
    NV_ALERT_STRING = NV_ALERT + ": {return_date}"
    DEFINED_ALERTS = [TBV_ALERT, NC_ALERT]  # TBV_ALERT, NC_ALERT, NV_ALERT

    for project_key in PROJECTS:
        project = redcap.Project(URL, PROJECTS[project_key])

        # Get all records for each ICARIA REDCap project
        print("[{}] Getting all records from {}...".format(datetime.now(), project_key))
        df = project.export_records(format='df')

        # Custom status
        custom_status_ids = alerts.get_record_ids_with_custom_status(df, DEFINED_ALERTS)

        # Households to be visited
        if TBV_ALERT in DEFINED_ALERTS:
            alerts.set_tbv_alerts(project, df, TBV_ALERT, TBV_ALERT_STRING, REDCAP_DATE_FORMAT, ALERT_DATE_FORMAT,
                                  CHOICE_SEP, CODE_SEP, custom_status_ids)

        # Update REDCap data as it has may been modified by set_tbv_alerts
        df = project.export_records(format='df')

        # Non-compliant visits
        if NC_ALERT in DEFINED_ALERTS:
            alerts.set_nc_alerts(project, df, NC_ALERT, NC_ALERT_STRING, CHOICE_SEP, CODE_SEP, DAYS_TO_NC,
                                 custom_status_ids)

        # Update REDCap data as it has may been modified by set_nc_alerts
        df = project.export_records(format='df')

        # Next visit
        if NV_ALERT in DEFINED_ALERTS:
            alerts.set_nv_alerts(project, df, NV_ALERT, NV_ALERT_STRING, ALERT_DATE_FORMAT, DAYS_BEFORE_NV,
                                 DAYS_AFTER_NV, custom_status_ids)
