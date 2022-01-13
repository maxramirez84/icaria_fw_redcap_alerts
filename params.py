import tokens

# REDCap parameters
URL = tokens.URL
TRIAL_PROJECTS = tokens.REDCAP_PROJECTS_ICARIA
COHORT_PROJECTS = tokens.REDCAP_PROJECTS_COHORT
CHOICE_SEP = " | "
CODE_SEP = ", "
REDCAP_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
TRIAL_CHILD_FU_STATUS_EVENT = "epipenta1_v0_recru_arm_1"
COHORT_CHILD_FU_STATUS_EVENT = "ipti_1__10_weeks_r_arm_1"

# Alerts general parameters
ALERT_DATE_FORMAT = "%b %d"

# TO BE VISITED ALERT
TBV_ALERT = "TBV"
TBV_ALERT_STRING = TBV_ALERT + "@{community} AZi/Pbo@{last_azi_date}"

# NON-COMPLIANT ALERT
DAYS_TO_NC = 28  # Defined by PI as 4 weeks
NC_ALERT = "NC"
NC_ALERT_STRING = NC_ALERT + "@{community} ({weeks} weeks)"

# NEXT VISIT ALERT
DAYS_BEFORE_NV = 7  # Defined by In-Country Technical Coordinator
DAYS_AFTER_NV = DAYS_TO_NC  # Defined by In-Country Technical Coordinator
NV_ALERT = "NEXT VISIT"
NV_ALERT_STRING = NV_ALERT + ": {return_date}"

# END OF FOLLOW UP
DAYS_BEFORE_END_FU = 7  # Defined by In-Country Technical Coordinator
END_FU_ALERT = "END F/U"
END_FU_ALERT_STRING = END_FU_ALERT + ": {birthday}"
END_FU_TRIAL = 18   # By protocol
END_FU_COHORT = 15  # By protocol

# ENABLED ALERTS: Subset of [TBV_ALERT, NC_ALERT, NV_ALERT, END_FU_ALERT]
TRIAL_DEFINED_ALERTS = [TBV_ALERT, NC_ALERT, END_FU_ALERT]
COHORT_DEFINED_ALERTS = [END_FU_ALERT]
