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


def get_list_communities(redcap_project):
    """Get list of communities in the health facility catchment area from the health facility REDCap project. This list
    is part of the metadata of the ID.community field.

    :param redcap_project: The REDCap project class
    :type redcap_project: redcap.Project

    :return: A dictionary in which the keys are the community code and the values are the community names.
    :rtype: dict
    """
    print("Getting the list of communities in the {} catchment area...".format(project_key))
    community_field = redcap_project.export_metadata(fields=['community'], format='df')
    community_choices = community_field['select_choices_or_calculations'].community
    communities_string = community_choices.split(CHOICE_SEP)
    return {community.split(CODE_SEP)[0]: community.split(CODE_SEP)[1] for community in communities_string}


def get_record_ids_tbv(redcap_data):
    """Get the project record ids of the participants requiring a household visit. Thus, for every project record, check
    if the number of AZi/Pbo doses is higher than the number of household visits (excluding Non-Compliant visits) in
    which the field worker has seen the child. This is therefore the AZi-Supervision index:
         - Higher than zero: Participant requires a household follow up visit;
         - Zero: Participant who has been correctly supervised;
         - Lower than zero: Participant who has ended her follow up.

    :param redcap_data: Exported REDCap project data
    :type redcap_data: pandas.DataFrame

    :return: Array of record ids representing those study participants that require a AZi/Pbo supervision household
    visit
    :rtype: pandas.Int64Index
    """
    azi_doses = redcap_data.groupby('record_id')['int_azi'].sum()
    times_hh_child_seen = redcap_data.groupby('record_id')['hh_child_seen'].sum()
    azi_supervision = azi_doses - times_hh_child_seen

    return azi_supervision[azi_supervision > 0].keys()


def build_fw_alerts_df(redcap_data, record_ids):
    """Build dataframe with record ids, communities, date of last AZi/Pbo dose and follow up status of every study
    participant requiring an AZi/Pbo supervision household visit.

    :param redcap_data:Exported REDCap project data
    :type redcap_data: pandas.DataFrame
    :param record_ids: Array of record ids representing those study participants that require a AZi/Pbo supervision
    household visit
    :type record_ids: pandas.Int64Index

    :return: A dataframe with the columns community, last_azi_date and child_fu_status in which each row is identified
    by the REDCap record id and represents a study participant to be visited.
    :rtype: pandas.DataFrame
    """
    # Append to record ids, the participant's community name
    communities_to_be_visited = redcap_data['community'][record_ids]
    communities_to_be_visited = communities_to_be_visited[communities_to_be_visited.notnull()]
    communities_to_be_visited = communities_to_be_visited.apply(int).apply(str).replace(communities)
    communities_to_be_visited.index = communities_to_be_visited.index.get_level_values('record_id')

    # Append to record ids, the date of last AZi/Pbo dose administered to the participant
    last_azi_doses = redcap_data.loc[records_to_be_visited, ['int_azi', 'int_date']]
    last_azi_doses = last_azi_doses[last_azi_doses['int_azi'] == 1]
    last_azi_doses = last_azi_doses.groupby('record_id')['int_date'].max()
    last_azi_doses = last_azi_doses.apply(lambda x: datetime.strptime(x, REDCAP_DATE_FORMAT))
    last_azi_doses = last_azi_doses.apply(lambda x: x.strftime(ALERT_DATE_FORMAT))

    # Transform data to be imported into the child_status_fu variable into the REDCap project
    data = {'community': communities_to_be_visited, 'last_azi_date': last_azi_doses}
    data_to_import = pandas.DataFrame(data)
    data_to_import['child_fu_status'] = data_to_import[['community', 'last_azi_date']].apply(
        lambda x: TBV_ALERT.format(community=x[0], last_azi_date=x[1]), axis=1)

    return data_to_import


def get_active_alerts(redcap_data):
    """Get the project records ids of the participants with an activated alert.

    :param redcap_data: Exported REDCap project data
    :type redcap_data: pandas.DataFrame

    :return: A series containing the record ids and alerts of the study participants who have an activated alert.
    :rtype: pandas.Series
    """
    active_alerts = redcap_data.loc[(slice(None), 'epipenta1_v0_recru_arm_1'), 'child_fu_status']
    active_alerts = active_alerts[active_alerts.notnull()]
    active_alerts.index = active_alerts.index.get_level_values('record_id')

    return active_alerts


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
        communities = get_list_communities(project)

        # Get all records for each ICARIA REDCap project
        print("Getting all records from {}...".format(project_key))
        df = project.export_records(format='df')

        # Get the project records ids of the participants requiring a household visit
        records_to_be_visited = get_record_ids_tbv(df)

        # Get the project records ids of the participants with an active alert
        records_with_alerts = get_active_alerts(df)

        # Build dataframe with fields to be imported into REDCap (record_id and child_fu_status)
        to_import_df = build_fw_alerts_df(df, records_to_be_visited)

        # Import data into the REDCap project
        to_import_dict = [{'record_id': rec_id, 'child_fu_status': participant.child_fu_status}
                          for rec_id, participant in to_import_df.iterrows()]
        response = project.import_records(to_import_dict)
        print(response)
