import tomllib

# that how you read a toml file, industry standard now
with open('metabase.toml', 'rb') as f:  # note : 'rb' mode for tomllib
    config = tomllib.load(f)

# postgres config
postgres_host = config['databases']['postgres-local']['host']
postgres_port = config['databases']['postgres-local']['port']
postgres_db = config['databases']['postgres-local']['dbname']
postgres_user = config['databases']['postgres-local']['user']
postgres_password = config['databases']['postgres-local']['password']


print(f"Connection config imported sucessfully as follows :")
print(f"Host: {postgres_host}")  # host.docker.internal
print(f"Port: {postgres_port}")  # 5432
print(f"DB: {postgres_db}")      # mydb
