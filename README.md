Для генерации полного файла со 100 БД:

#!/bin/bash
echo "replication:" > application.yml
echo "  source-databases:" >> application.yml

for i in {1..100}; do
  cat <<EOF >> application.yml
    - name: source_db_$i
      url: jdbc:postgresql://source-host-$i:5432/source_db_$i
      username: repl_user
      password: repl_password
      schema: public
      priority: $(( (i-1)/50 + 1 ))
      active: true
      connection-pool:
        max-total: 10
        max-idle: 5
        min-idle: 2
        max-wait-ms: 10000
EOF
done
