# Sujet du projet

Notre entreprises souhaite lancer un nouveau produit SAS et souhaite pour cela savoir quel language de programmation utiliser. Idéalement, ce langage devrait être populaire, utilisé par un noumbre important de personnes, et avec une communauté actives. 

Via les données disponible sur l'API Github, vous réaliserez les pipelines de données permettant de réaliser les analyses suivantes: 

"En se basant sur plusieurs métriques associé aux repositories et gists accessible publiquement sur GitHub (commits, contributeurs distinct, issues, nombre de stars, pull requests, forks), quels sont le(s) langage(s) de programmation à utlliser pour notre nouveau produit SAS ?

Pour réaliser cette analyse, vous créerez les différents services d'acquisition ( acquisition des données sur les gists / repositories, puis acquisition des données associées) et de transformation des données sans oublier les différents stade de stockage de la données.

Dans ce repository, vous trouver un example de pipelines ETL en vue d'une analyse sur les GIST Github. 

## Acquisition de la données

Pour acquérir la données, vous utiliserez l'API GitHub REST ou GRAPHQL. 

Voir :
- https://docs.github.com/fr/rest
- https://docs.github.com/fr/graphql

## Stockage de la données 

Comme nous n'avons pas l'environnment technique qui nous ne permet, nous ne gérerons pas de Data Lake.

Une base de données Postgresql sera utilisé comme Data Warehouse

## Transformation de la données

Transformer la données pour quelle soit agrégés et prête à être utilisé pour des analyses. Une modélisation dimensionel peut être réalisé (mais ce n'est pas obligatoire)


## Resources:

### Digital Ocean

- https://docs.digitalocean.com/reference/api/spaces-api/
- https://docs.digitalocean.com/reference/api/api-try-it-now/

### Dagster

- https://docs.dagster.io/getting-started
- https://docs.dagster.io/getting-started/install

### Github

- https://docs.github.com/fr/rest
- https://docs.github.com/fr/graphql

### Cours 

- https://docs.google.com/presentation/d/1wuf280DJ6HDqAVaRl9dNtyd2-755Gh9ZsIW-0gw1RKk/edit?usp=sharing