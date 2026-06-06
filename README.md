# Pipeline Big Data : Kafka + Spark + ELK Stack

Projet de traitement de logs en temps réel basé sur Apache Kafka, Apache Spark Streaming et la stack ELK (Elasticsearch, Logstash, Kibana).

## Architecture

```
VMs Linux
   │
   ▼
Filebeat  ──────────►  Kafka (Topic: vm_logs)
                              │
                    ┌─────────┴─────────┐
                    ▼                   ▼
              Spark Streaming      Logstash
              (Classification)         │
                    │                  ▼
                    └──────►  Elasticsearch
                                       │
                                       ▼
                                   Kibana
```

## Composants

### 1. Collecte — Filebeat
`kafkaetELKstack/script_collecte_dedonnées/filebeat.inputs.txt`

Filebeat est déployé sur chaque VM pour collecter les logs système (`/var/log/syslog`, `/var/log/auth.log`) et les envoyer vers le topic Kafka `vm_logs`.

### 2. Traitement — Apache Spark
`FicherPythonKafkaSpark.ipynb`

Trois scripts Spark Streaming :
- **Producteur** : génère des logs simulés et les publie sur Kafka
- **Consumer** : lit le flux Kafka et calcule des statistiques par niveau de log
- **Classificateur** : catégorise les logs en `Critical`, `Warning`, `Information`

### 3. Indexation — Logstash + Elasticsearch
`kafkaetELKstack/classificationetliaison/Classification.txt`

Pipeline Logstash qui :
- Consomme depuis Kafka
- Parse les logs avec Grok
- Normalise les niveaux (`ERR` → `ERROR`, etc.)
- Indexe dans Elasticsearch (`vm_logs-YYYY.MM.dd` et `vm_critical_logs-YYYY.MM.dd`)

## Prérequis

- Java 11+
- Apache Kafka 3.x
- Apache Spark 3.5+
- Elasticsearch 8.x + Logstash 8.x + Kibana 8.x
- Filebeat 8.x
- Python 3.9+

```bash
pip install -r requirements.txt
```

## Lancement

### Démarrer Kafka
```bash
# Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Broker Kafka
bin/kafka-server-start.sh config/server.properties

# Créer le topic
bin/kafka-topics.sh --create --topic vm_logs --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### Configurer Filebeat
Copier le contenu de `filebeat.inputs.txt` dans `/etc/filebeat/filebeat.yml` sur chaque VM, en adaptant :
- `vm_name` : nom de la VM
- `hosts` dans `output.kafka` : IP du serveur Kafka

```bash
sudo systemctl start filebeat
```

### Lancer Logstash
```bash
bin/logstash -f kafkaetELKstack/classificationetliaison/Classification.txt
```

### Lancer Spark
Ouvrir `FicherPythonKafkaSpark.ipynb` dans Jupyter et remplacer `<VOTRE_IP>` par l'IP du master Spark, puis exécuter les cellules.

### Visualisation
Accéder à Kibana sur `http://localhost:5601` et créer un index pattern `vm_logs-*`.

## Structure du projet

```
projetbigdata/
├── FicherPythonKafkaSpark.ipynb        # Scripts Kafka Producer + Spark Consumer + Classificateur
├── kafkaetELKstack/
│   ├── script_collecte_dedonnées/
│   │   └── filebeat.inputs.txt         # Configuration Filebeat
│   └── classificationetliaison/
│       └── Classification.txt          # Pipeline Logstash
└── requirements.txt
```

## Technologies

| Outil | Rôle |
|---|---|
| Filebeat | Collecte de logs sur les VMs |
| Apache Kafka | Bus de messages distribué |
| Apache Spark Streaming | Traitement temps réel |
| Logstash | Parsing et enrichissement |
| Elasticsearch | Stockage et indexation |
| Kibana | Visualisation |
