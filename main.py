#!/usr/bin/env python
""" Python script to setup alerts for ICARIA field workers. These alerts are for them to know if they have to do a
household visit after AZi/Pbo administration or a Non-Compliant visit. In the context of the ICARIA Clinical Trial, a
household visit is scheduled few days after the administration of the investigational product (azithromycin in this
case). Moreover, if study participants are not coming to the scheduled study visits, another household visit will be
scheduled to capture their status. This script is computing regularly which of the participants requires a household or
Non-Compliant visit. This requirement is saved into an eCRF variable in the Screening DCI. This variable will be setup
as part of the REDCap custom record label. Like this, field workers will see in a glance which participants they need to
visit at their households."""
import pandas
import redcap
import tokens
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

    for project_key in PROJECTS:
        # Get all records for each ICARIA REDCap project
        print("Getting all records from {}...".format(project_key))
        project = redcap.Project(URL, PROJECTS[project_key])
        df = project.export_records(format='df')

        # For every project record, check if the number of AZi/Pbo doses is higher than the number of household visits
        # (excluding Non-Compliant visits) in which the field worker has seen the child
        azi_doses = df.groupby('record_id')['int_azi'].sum()
        times_hh_child_seen = df.groupby('record_id')['hh_child_seen'].sum()
        azi_supervision = azi_doses - times_hh_child_seen

        # Get the project records of the participants requiring a household visit (those in which the azi_supervision
        # index is higher than 0; 0 means correctly supervised; -1 means end of follow up) and update their status
        records_to_be_visited = azi_supervision[azi_supervision > 0].keys()
        to_import = [{'record_id': record_id, 'child_fu_status': 'TBV'} for record_id in records_to_be_visited]
        response = project.import_records(to_import)
        print(response)


