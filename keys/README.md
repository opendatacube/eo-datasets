# Fake Keys for AWS S3 Testing

Generate a CA key pair:

    cfssl gencert -initca ca-csr.json | cfssljson -bare ca -

Generate a key for *.s3.amazonws.com:

    cfssl gencert   -ca=ca.pem   -ca-key=ca-key.pem   -config=ca-config.json
    -profile=web-servers   server-csr.json | cfssljson -bare server
