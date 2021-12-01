import tokens

# REDCap parameters
URL = tokens.URL
PROJECTS = tokens.REDCAP_PROJECTS
CHOICE_SEP = " | "
CODE_SEP = ", "
REDCAP_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

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

# ENABLED ALERTS
DEFINED_ALERTS = [TBV_ALERT, NC_ALERT]  # TBV_ALERT, NC_ALERT, NV_ALERT
