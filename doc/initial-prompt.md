# Prompt initial du projet — Morning Review

> Date de consignation : 2026-11-13

## Expression de besoin originale

```text
JE souhaite constuire un système dédié à ma veille et au morning review
j'anticipe les besoins suivants:
- aggrégation de sources, donc des contenus issu de newsletter de curration (dans une boite gmail, donné dans .env), des flux rss, des sites web, twitter/X, masi abonnement payant (pour la science, new scientist, canarpc, et...)
- un rangement par catégories (tech, perso, ...)
- un besoin d'identifier les sujets par un idunique, conservé en base de données afin de gérer les sources multiples
- pouvoir chercher dans les sources par la suite, par mot clé ou par recherche approchée sémantique
- pouvoir indiquer si un sujet est pertinent ou pas pour m'auto-profiler
- pouvoir présenter un rapport quotidien, dans un mail, dans un site web persisitant et un repo git
- sur un système à la fois déterministe et via un LLM pour l'analyse du contenu textuel
- utiliser des recommendations externes comme source (par exemple la home de mon youtube, spotify, facebook, twitter/X)
- j'anticipe que la puissance de calcul coté LLM ou vLLM n'est pas énorme, et la criticité des informations faible (car la pluspart du temps publique), une clé sur openrouter via des modèles gratuit doit être ok
- je veux une architecture produit simple mais efficace, la base de donnée notamment doit être approprié à du stockage et au requétage, plus un stockage probablement texte sur du S2
- je veux que ce même produit puisse servir à d'autre, donc inclure une approche multi-utilisateurs, qui ne doublonnera pas toutes les sources
- je veux que cela s'appuie sur la stack techno que j'utilise le plus en ce moment (k8s, s3, sqlite et/ou postgres, vuejs, node ou python)

Ton travail est de proposer un plan solide avant de commencer, et de le stocker dans ce repo afin que nous puissions enchaine par la suite.
pose tes questions avant de commencer
```
