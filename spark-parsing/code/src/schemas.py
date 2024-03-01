

HOSTS_DDL = 'id INT NOT NULL, ip STRING NOT NULL, port INT NOT NULL, server_name STRING,' \
            'synStart INT, synEnd INT, scanEnd INT, protocol INT, cipher STRING,' \
            'resultString STRING, error_data STRING, cert_id INT, cert_hash STRING, pub_key_hash STRING, cert_valid INT,' \
            'tls_alerts_send STRING, peer_certificates STRING, tls_alerts_received STRING, client_hello STRING'
HOSTS_RELEVANT_COLUMNS = ['id', 'ip', 'server_name', 'error_data', 'cert_hash', 'cert_valid']

CERTS_DDL = 'id int, cert string, system_cert_store int'

CERTS_PARSED_DDL = 'id int, sha1 string, sha256 string, valid boolean, verifyError STRING,' \
                   'subject string, issuer string, notBefore string, notAfter string, basicConstraints boolean, isCa boolean,' \
                   'sha256PubKey string, numAltNames int, altNames string'
CERTS_RELEVANT_COLUMNS = ['id', 'sha1', 'sha256', 'subject', 'issuer', 'notBefore', 'notAfter', 'basicConstraints', 'isCa', 'numAltNames', 'altNames']
