# PARQUETFLOW

## SOMMAIRE
- [INTRODUCTION](#introduction)
- [PYTHON](#python)
- [REQUIREMENTS](#requirements)
- [GETTING STARTED](#getting-started)
- [TO DO](#to-do)

## INTRODUCTION
Dans un monde où les volumes de données augmentent de manière exponentielle, leurs collectes deviennent des enjeux cruciaux. Ce script Python se positionne comme un outil clé dans cette démarche, en automatisant la récupération de données à partir d’une URL (qu’elle provienne d’une API REST ou d’un autre endpoint web) et leur transformation au format Parquet, reconnu pour sa performance en termes de compression et de lecture.  
Grâce à l'utilisation de la bibliothèque Polars, réputée pour sa rapidité et sa gestion efficace des données tabulaires, ce script garantit un traitement fluide, performant et évolutif, même pour des ensembles de données volumineux.  
Les fonctionnalités incluent :
- La récupération des données à partir d’une source en ligne via une URL (avec prise en charge des formats JSON ou CSV).
- La transformation des données brutes en un modèle structuré et optimisé pour l'analyse ou le traitement.
- L’export des données transformées au format Parquet, prêt à être utilisé pour des pipelines d'analyse ou de traitement avancé.
- Ajouter d'un token d'authentification à la requête, si cela est nécessaire, afin d'accéder aux données.
- De plus il est possible d'enregistrer ce token en variable d'environnement pour une utilisation ultérieure.
- Ajout du token dans le header fait planter le scrapping sur les météorites.  

Cet outil est idéal pour les développeurs, data engineers ou analystes cherchant à automatiser la gestion de leurs flux de données. Il répond particulièrement bien aux besoins des projets nécessitant une gestion performante et évolutive des données tabulaires.

## PYTHON
[Télécharger Python 3.13.1](https://www.python.org/downloads/)  

⚠️ "Customize installation"  

Cocher les options =>  
- "pip"  
- "tcl/tk and IDLE"  
- "py launcher"  

![Installation Python 1](https://github.com/EmmanuelLefevre/MarkdownImg/blob/main/py_install.png)  

Puis dans la seconde fenêtre =>  
- "Associate files with Python"  
- "Add Python to environment variables".  

![Installation Python 2](https://github.com/EmmanuelLefevre/MarkdownImg/blob/main/py_install_2.png)  

- Vérifier l'installation de Python
```bash
python --version
```
- Vérifier l'installation de Pip
```bash
pip --version
```

## REQUIREMENTS
- Colorama
- Pandas
- Pyarrow
- Python-dotenv
- Requests

## GETTING STARTED
1. Installer les librairies (en local dans python)
```bash
pip install -r requirements.txt
```
2. Vérifier l'installation des librairies
```bash
pip list
```
3. Lancer l'application python
```bash
python app.py
```
4. URL de test
- JSON
**Avec paramètres de page**  
URL de test permettant de récupérer tous les contributeurs du projet VsCode sur github.com
```bash
https://api.github.com/repos/microsoft/vscode/contributors
```
**Sans paramètres de page**  
URL de test permettant de récupérer, depuis l'API du gouvernement américain, la population américaine par année.
```bash
https://datausa.io/api/data?drilldowns=Nation&measures=Population
```
- CSV
**Avec paramètres de limite**  
URL de test permettant de récupérer mondialement, depuis l'API de la NASA, les points d'impact des météorites (+ autres données associées).
```bash
https://data.nasa.gov/resource/gh4g-9sfh.csv?$limit=50000
```
5. Les fichiers Parquet des exemples ci dessus sont disponibles dans le dossier "data_frame".

## TO DO
- Utiliser Polars à la place de panda
- Ajouter un prompt pour utiliser ou pas le token existant
- Ajouter fonction de contrôle champ de saisie faux
- Redemander le nombre de résultats à récupérer si seconde url saisie
- Saisir les paramètres dans un prompt => NASA le paramètre à passer dans l'url est "?$limit=50000"
- Check le ValueError qui n'est pas dans un bloc except
- Ajouter un prompt pour demander si l'on continue toutes les 100 pages

***

⭐⭐⭐ I hope you enjoy it, if so don't hesitate to leave a like on this repository and on the [Dotfiles](https://github.com/EmmanuelLefevre/Dotfiles) one (click on the "Star" button at the top right of the repository page). Thanks 🤗
