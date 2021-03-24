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
    TBV_ALERT = "TBV@{community} AZi/Pbo@{last_azi_date}"
    CHOICE_SEP = " | "
    CODE_SEP = ", "
    REDCAP_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    ALERT_DATE_FORMAT = "%b %d"

    for project_key in PROJECTS:
        project = redcap.Project(URL, PROJECTS[project_key])

        # Get list of communities in the health facility catchment area
        print("Getting the list of communities in the {} catchment area...".format(project_key))
        community_field = project.export_metadata(fields=['community'], format='df')
        community_choices = community_field['select_choices_or_calculations'].community
        communities_string = community_choices.split(CHOICE_SEP)
        communities = {community.split(CODE_SEP)[0]: community.split(CODE_SEP)[1] for community in communities_string}

        # Get all records for each ICARIA REDCap project
        print("Getting all records from {}...".format(project_key))
        df = project.export_records(format='df')

        # For every project record, check if the number of AZi/Pbo doses is higher than the number of household visits
        # (excluding Non-Compliant visits) in which the field worker has seen the child
        azi_doses = df.groupby('record_id')['int_azi'].sum()
        times_hh_child_seen = df.groupby('record_id')['hh_child_seen'].sum()
        azi_supervision = azi_doses - times_hh_child_seen

        # Get the project records of the participants requiring a household visit (those in which the azi_supervision
        # index is higher than 0; 0 means correctly supervised; -1 means end of follow up) and update their status
        records_to_be_visited = azi_supervision[azi_supervision > 0].keys()

        # Append to record ids, the participant's community name and date of last AZi/Pbo dose
        communities_to_be_visited = df['community'][records_to_be_visited]
        communities_to_be_visited = communities_to_be_visited[communities_to_be_visited.notnull()]
        communities_to_be_visited = communities_to_be_visited.apply(int).apply(str).replace(communities)
        communities_to_be_visited.index = communities_to_be_visited.index.get_level_values('record_id')

        last_azi_doses = df.loc[records_to_be_visited, ['int_azi', 'int_date']]
        last_azi_doses = last_azi_doses[last_azi_doses['int_azi'] == 1]
        last_azi_doses = last_azi_doses.groupby('record_id')['int_date'].max()
        last_azi_doses = last_azi_doses.apply(lambda x: datetime.strptime(x, REDCAP_DATE_FORMAT))
        last_azi_doses = last_azi_doses.apply(lambda x: x.strftime(ALERT_DATE_FORMAT))

        data = {'community': communities_to_be_visited, 'last_azi_date': last_azi_doses}
        data_to_import = pandas.DataFrame(data)
        data_to_import['child_fu_status'] = data_to_import[['community', 'last_azi_date']].apply(
            lambda x: TBV_ALERT.format(community=x[0], last_azi_date=x[1]), axis=1)

        to_import = [{'record_id': rec_id, 'child_fu_status': participant.child_fu_status}
                     for rec_id, participant in data_to_import.iterrows()]
        response = project.import_records(to_import)
        print(response)
