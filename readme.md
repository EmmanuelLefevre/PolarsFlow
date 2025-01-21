# POLARSFLOW

## SOMMAIRE
- [INTRODUCTION](#introduction)
- [PYTHON](#python)
- [REQUIREMENTS](#requirements)
- [GETTING STARTED](#getting-started)

## INTRODUCTION
Dans un monde où les volumes de données augmentent de manière exponentielle, leurs collectes deviennent des enjeux cruciaux. Ce script Python se positionne comme un outil clé dans cette démarche, en automatisant la récupération de données à partir d’une URL (qu’elle provienne d’une API REST, GraphQL ou d’un autre endpoint web) et leur transformation au format Parquet, reconnu pour sa performance en termes de compression et de lecture.  
Grâce à l'utilisation de la bibliothèque Polars, réputée pour sa rapidité et sa gestion efficace des données tabulaires, ce script garantit un traitement fluide, performant et évolutif, même pour des ensembles de données volumineux.  
Les principales fonctionnalités incluent :
- La récupération des données à partir d’une source en ligne via une URL (avec prise en charge des formats JSON ou CSV).
- La transformation des données brutes en un modèle structuré et optimisé.
- L’export des données transformées au format Parquet, prêt à être utilisé pour des pipelines d'analyse ou de traitement avancé.  
Cet outil est idéal pour les développeurs, data engineers ou analystes cherchant à automatiser la gestion de leurs flux de données tout en maximisant la performance et la fluidité des opérations.

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
- Python-dotenv
- Requests

## GETTING STARTED
1. Installer les librairies (en local dans python)
```bash
pip install -r requirements.txt
```
Vérifier l'installation des librairies
```bash
pip list
```
2. Créer un .env à partir du .env.template et changer **MANUELLEMENT** les valeurs pertinentes
```bash
cp .env.template .env
```
3. Lancer l'application python
```bash
python app.py
```

***

⭐⭐⭐ I hope you enjoy it, if so don't hesitate to leave a like on this repository and on the [Dotfiles](https://github.com/EmmanuelLefevre/Dotfiles) one (click on the "Star" button at the top right of the repository page). Thanks 🤗
