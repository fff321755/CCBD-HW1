# add data

curl -XPOST -u [MASTER_NAME]:[MASTER_PASSWORD] [DOMAIN_ENDPOINT]/_bulk --data-binary @[JSON_FILENAME] -H 'Content-Type: application/json'

curl -XPOST -u master:Passw0rd#  https://search-restaurants-sdbsgufge7frswmfnex6i2nkty.us-east-1.es.amazonaws.com/_bulk --data-binary @esdata.json -H 'Content-Type: application/json'



# Delete All match_all -> match to delete specifict data

curl -XPOST -u master:'Passw0rd#' 'https://search-restaurants-sdbsgufge7frswmfnex6i2nkty.us-east-1.es.amazonaws.com/restaurant/_delete_by_query'  -d'
{
  "query": {
    "match_all": {
    }
  }
}' -H 'Content-Type: application/json'