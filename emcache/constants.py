import re

regex_memcached_responses = re.compile(
    r"(?P<error_message>ERROR [\w ]+)|"
    r"(?P<client_error>CLIENT_ERROR [\w ]+)|"
    r"(?P<server_error>SERVER_ERROR [\w ]+)|"
    r"(?P<error>ERROR)|"
    r"(?P<stored>STORED)|"
    r"(?P<not_stored>NOT_STORED)|"
    r"(?P<exists>EXISTS)|"
    r"(?P<not_found>NOT_FOUND)|"
    r"(?P<value>(VALUE (?P<key>\w+) (?P<flags>\d+) \d+(?P<cas> \d+)?\r\n(?P<data>\w+\r\n)+END))|"
    r"(?P<end>END)|"
    r"(?P<deleted>DELETED)|"
    r"(?P<touched>TOUCHED)|"
    r"(?P<version>VERSION \d+\.\d+\.\d+)|"
    r"(?P<ok>OK)|"
    r"(?P<stats>(STAT (.+) (.+)\r\n)+END)|"
    r"(?P<incr_decr_value>\d+)"
)
