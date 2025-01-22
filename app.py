import os
import requests
import sys
import pandas as pd
# import pyarrow.parquet as pq

from colorama import Fore, Style, init
from dotenv import load_dotenv
from tkinter import Tk
from tkinter.filedialog import asksaveasfilename



################
##### Load #####
################
# Charger variables d'environnement
load_dotenv()

# Créer instance de Tk
tkInstance = Tk()
tkInstance.withdraw()

# Variable globale pour stocker l'URL
last_url = None

# Initialiser colorama
init()



##############################################
##### Fonction pour quitter le programme #####
##############################################
def leave():
  print(f"{Style.BRIGHT}{Fore.BLUE}👋 Programme terminé.{Style.RESET_ALL}")
  sys.exit(0)



###################################################
##### Fonction pour récupérer le secret token #####
###################################################
def get_secret_token(url=None):
  token = os.getenv("SECRET_TOKEN")

  if token == "":
    print(f"{Style.BRIGHT}{Fore.RED}💣 Ce token n'a pas de valeur définie !{Style.RESET_ALL}")
    set_secret_token(url)
    token = os.getenv("SECRET_TOKEN")

  if not token:
    print(f"{Style.BRIGHT}{Fore.RED}💣 Aucun token trouvé dans le fichier .env !{Style.RESET_ALL}")
    response = input("🏁 Avez-vous un token à renseigner ? (O/n/e=enregistrer) : ").strip().lower()

    if response == "e":
      set_secret_token(url)
      token = os.getenv("SECRET_TOKEN")

    elif response in ["o", ""]:
      token = input("💬 Entrez votre token : ").strip()

  return token


####################################################
##### Fonction pour renseigner un secret token #####
####################################################
def set_secret_token(url=None):
  global last_url
  last_url = url

  while True:
    secret_token = input("💬 Entrez votre token ('fin' pour quitter) : ").strip()

    if secret_token.lower() == "fin":
      leave()

    if secret_token:
      # Créer le fichier .env uniquement si un token est fourni
      if not os.path.exists(".env"):
        with open(".env", "w") as f:
          f.write("")

      # Lire le contenu du fichier .env
      with open(".env", "r") as f:
        lines = f.readlines()

      # Mettre à jour ou ajouter la ligne SECRET_TOKEN=
      new_lines = []
      token_exists = False
      for line in lines:
        if line.startswith("SECRET_TOKEN="):
          new_lines.append(f"SECRET_TOKEN={secret_token}")
          token_exists = True
        else:
          new_lines.append(line)

      # Si token n'existe pas => l'ajouter
      if not token_exists:
        new_lines.append(f"SECRET_TOKEN={secret_token}")

      # Si .env modifié => réécrire fichier
      with open(".env", "w") as f:
        f.writelines(new_lines)

      # Enregistrer token dans .env
      os.environ["SECRET_TOKEN"] = secret_token
      print(f"{Style.BRIGHT}{Fore.GREEN}✅ Token enregistré dans .env{Style.RESET_ALL}")

      # Quitter la boucle si un token est saisi
      break

  # Relancer la requête
  api_call(last_url)



########################################################
##### Fonction pour enregistrer le fichier Parquet #####
########################################################
def save_file(df):
  try:
    print("📂 Sélectionner un emplacement pour sauvegarder le fichier.")
    save_path = asksaveasfilename(
      title="Enregistrer le fichier Parquet",
      defaultextension=".parquet",
      filetypes=[("Fichiers Parquet", "*.parquet")],
      initialdir=os.path.join(os.getcwd(), "data_frame")
    )

    if save_path:
      # Ajouter automatiquement l'extension ".parquet" si absente
      if not save_path.endswith(".parquet"):
        save_path += ".parquet"

      # Extraire le nom de fichier et l'extension
      filename, extension = os.path.splitext(os.path.basename(save_path))

      # Sauvegarder le DataFrame au chemin sélectionné
      df.to_parquet(save_path, engine="pyarrow", index=False)
      print(f"{Style.BRIGHT}{Fore.GREEN}📄 {filename}{extension} enregistré sous: {save_path}{Style.RESET_ALL}")
    else:
      print(f"{Style.BRIGHT}{Fore.RED}❌ Sauvegarde annulée par l'utilisateur...{Style.RESET_ALL}")

  except PermissionError:
    print(f"{Style.BRIGHT}{Fore.RED}💣 Fichier ouvert, assurez-vous que celui-ci est fermé !{Style.RESET_ALL}")

  except Exception as e:
    print(f"{Style.BRIGHT}{Fore.RED}💣 Erreur lors de la sauvegarde : {e}{Style.RESET_ALL}")



######################################################
##### Fonction pour convertir le JSON en Parquet #####
######################################################
def convert_json_to_parquet(json_data):
  try:
    # Vérifier si le JSON est bien une liste ou un dictionnaire
    if not isinstance(json_data, (list, dict)):
      raise ValueError(f"{Style.BRIGHT}{Fore.RED}💣 Les données JSON doivent être une liste ou un dictionnaire !{Style.RESET_ALL}")

    # Convertir le JSON en DataFrame Pandas
    df = pd.DataFrame(json_data)

    # Enregistrer le fichier
    save_file(df)

  except ValueError as ve:
    print(f"{Style.BRIGHT}{Fore.RED}💣 Erreur de conversion : {ve}{Style.RESET_ALL}")
  except Exception as e:
    print(f"{Style.BRIGHT}{Fore.RED}💥 Une erreur s'est produite : {e}{Style.RESET_ALL}")



#################################################################
##### Fonction pour choisir le nombre de résultats par page #####
#################################################################
def get_results_per_page():
  while True:
    try:
      results_per_page = input("💬 Combien de résultats par requête souhaitez-vous récupérer ? (max 100) : ").strip()

      # Vérification que l'entrée est un entier
      results_per_page = int(results_per_page)
      # Dans la limite de 100
      if results_per_page < 1 or results_per_page > 100:
        print(f"{Style.BRIGHT}{Fore.RED}💣 Saisir un nombre entre 1 et 100 !{Style.RESET_ALL}")
      else:
        return results_per_page
    except ValueError:
      print(f"{Style.BRIGHT}{Fore.RED}💣 Saisir entrer un nombre entier !{Style.RESET_ALL}")



##################################################
##### Fonction pour choisir l'URL à scrapper #####
##################################################
def api_call(url=None):
  global last_url

  if url is None:
    invalid_url = False

    while True:
      if not invalid_url:
        prompt_message = "💬 Entrez l'URL de l'API que vous souhaitez scrapper ('fin' pour quitter) : "
      else:
        prompt_message = "💬 Saisir une autre URL ('fin' pour quitter) : "

      # Demander à l'utilisateur de saisir une URL
      url = input(prompt_message).strip()

      if url.lower() == "fin":
        leave()

      if not (url.startswith("https://") or url.startswith("http://")):
        if not invalid_url:
          invalid_url = True
        print(f"{Style.BRIGHT}{Fore.RED}💣 URL invalide !{Style.RESET_ALL}")
        continue
      # Si URL correcte => on sort de la boucle
      break

    try:
      token = get_secret_token(url)

      # Ajout du header
      headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3+json"
      }

      # Nombre de résultats par page
      results_per_page = get_results_per_page()
      # Initialiser la page actuelle à 1
      page = 1
      # Liste des résultats
      results = []

      while True:
        # Ajouter paramètre de pagination à l'URL
        paginated_url = f"{url}?page={page}&per_page=={results_per_page}"

        response = requests.get(paginated_url, headers=headers)

        # Response
        if response.status_code == 200:
          print(f"{Style.BRIGHT}{Fore.GREEN}👌 Données récupérées... Page : {page}{Style.RESET_ALL}")
          json_data = response.json()
          # Ajouter les résultats de la page actuelle à la liste globale
          results.extend(json_data)

          # Vérifier si une autre page existe
          if 'next' in response.links:
            # Incrémentation => page suivante
            page += 1
          else:
            break

        elif response.status_code == 401:
          print(f"{Style.BRIGHT}{Fore.MAGENTA}💥 Unauthorized request ! Essayez avec un token...{Style.RESET_ALL}")
          set_secret_token(url)
          return

        elif response.status_code == 404:
          print(f"{Style.BRIGHT}{Fore.MAGENTA}👀 404 not found !{Style.RESET_ALL}")
          return

        else:
          print(f"{Style.BRIGHT}{Fore.RED}Échec avec le code de statut {response.status_code} : {response.text}{Style.RESET_ALL}")

      # Appeler la fonction pour convertir en Parquet
      convert_json_to_parquet(results)

      # Après l'enregistrement du fichier, demander une nouvelle URL
      response = input("💬 Souhaitez-vous saisir une nouvelle URL ? (O/n) : ").strip().lower()

      if response == "n":
        leave()
      else:
        api_call()

    except ValueError as ve:
      print(ve)
    except requests.exceptions.RequestException as e:
      print(f"{Style.BRIGHT}{Fore.RED}Erreur lors de l'appel à l'API : {e}{Style.RESET_ALL}")



################
##### Main #####
################
def main():
  api_call()



#####################
##### Execution #####
#####################
if __name__ == "__main__":
  try:
    main()
  except KeyboardInterrupt:
    print(f"{Style.BRIGHT}{Fore.RED}👋 Opération interrompue par l'utilisateur. Programme terminé.{Style.RESET_ALL}")
  finally:
    tkInstance.quit()
    tkInstance.destroy()
    sys.exit(0)



#####################################################
##### Fonction pour convertir le CSV en Parquet #####
#####################################################


##############################################################
##### Fonction pour déterminer le type de données reçues #####
##############################################################


####################################################
##### Fonction pour utiliser le token existant #####
####################################################
