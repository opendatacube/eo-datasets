

# See https://scriptcrunch.com/create-ca-tls-ssl-certificates-keys/ for details

ca.pem ca-key.pem ca.csr: ca-csr.json
	cfssl gencert -initca ca-csr.json | cfssljson -bare ca -

server.pem server-key.pem server.csr: ca.pem ca-key.pem ca-config.json server-csr.json
	cfssl gencert   -ca=ca.pem   -ca-key=ca-key.pem   -config=ca-config.json   -profile=web-servers   server-csr.json | cfssljson -bare server

