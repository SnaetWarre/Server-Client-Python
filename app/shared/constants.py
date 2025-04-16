#!/usr/bin/env python3
# Constants for the client-server application

# Network settings
SERVER_HOST = 'localhost'
SERVER_PORT = 8888

# Message types for communication
MSG_REGISTER = 'REGISTER'
MSG_LOGIN = 'LOGIN'
MSG_LOGOUT = 'LOGOUT'
MSG_QUERY = 'QUERY'
MSG_QUERY_RESULT = 'QUERY_RESULT'
MSG_SERVER_MESSAGE = 'SERVER_MESSAGE'
MSG_CLIENT_LIST = 'CLIENT_LIST'
MSG_CLIENT_INFO = 'CLIENT_INFO'
MSG_CLIENT_HISTORY = 'CLIENT_HISTORY'
MSG_QUERY_STATS = 'QUERY_STATS'
MSG_GET_METADATA = 'GET_METADATA'

# Query types
QUERY_AGE_DISTRIBUTION = 'age_distribution'
QUERY_TOP_CHARGE_GROUPS = 'top_charge_groups'
QUERY_ARRESTS_BY_AREA = 'arrests_by_area'
QUERY_ARRESTS_BY_TIME = 'arrests_by_time'
QUERY_ARRESTS_BY_MONTH = 'arrests_by_month'
QUERY_CHARGE_TYPES_BY_AREA = 'charge_types_by_area'
QUERY_ARRESTS_BY_GENDER = 'arrests_by_gender'
QUERY_ARRESTS_BY_AGE_RANGE = 'arrests_by_age_range'
QUERY_ARRESTS_BY_WEEKDAY = 'arrests_by_weekday'
QUERY_CORRELATION_ANALYSIS = 'correlation_analysis'

# Query descriptions
QUERY_DESCRIPTIONS = {
    QUERY_AGE_DISTRIBUTION: 'Age distribution of arrested individuals',
    QUERY_TOP_CHARGE_GROUPS: 'Top charge groups by frequency',
    QUERY_ARRESTS_BY_AREA: 'Arrests by geographic area',
    QUERY_ARRESTS_BY_TIME: 'Arrests by time of day',
    QUERY_ARRESTS_BY_MONTH: 'Arrests by month',
    QUERY_CHARGE_TYPES_BY_AREA: 'Charge types by area',
    QUERY_ARRESTS_BY_GENDER: 'Arrests by gender',
    QUERY_ARRESTS_BY_AGE_RANGE: 'Arrests by age range',
    QUERY_ARRESTS_BY_WEEKDAY: 'Arrests by weekday',
    QUERY_CORRELATION_ANALYSIS: 'Correlation analysis between features'
}

# Response status codes
STATUS_OK = 'OK'
STATUS_ERROR = 'ERROR' 