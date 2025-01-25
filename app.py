import os
import requests
import sys
import pandas as pd
# import pyarrow.parquet as pq

from colorama import Fore, Style, init
from dotenv import load_dotenv
from io import StringIO
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

# Variable globale pour initialiser la page
page = 1

# Variable globale pour le nombre de résultats par page
results_per_page = None

# Initialiser colorama
init()



##############################################
##### Fonction pour quitter le programme #####
##############################################
def leave():
  print(f"{Style.BRIGHT}{Fore.BLUE}👋 Programme terminé.{Style.RESET_ALL}")
  sys.exit(0)



############################################
##### Fonction pour récupérer le token #####
############################################
def get_secret_token(url=None):
  token = os.getenv("SECRET_TOKEN")

  # Token vide
  if token == "":
    print(f"{Style.BRIGHT}{Fore.RED}💣 Ce token n'a pas de valeur !{Style.RESET_ALL}")
    set_secret_token(url)
    token = os.getenv("SECRET_TOKEN")

  # Token valide trouvé
  if token:
    print(f"{Style.BRIGHT}{Fore.GREEN}🔑 Token existant détecté !{Style.RESET_ALL}")
    response = input(f"🏁 Souhaitez-vous utiliser ce token ? (O/n/a=autre) : ").strip().lower()

    # Utiliser token existant
    if response in ["o", ""]:
      print(f"{Style.BRIGHT}{Fore.GREEN}✅ Utilisation du token existant.{Style.RESET_ALL}")
      return token

    # Utiliser un autre token
    elif response == "a":
      other_token = input("💬 Entrez un autre token : ").strip()
      if other_token:
        print(f"{Style.BRIGHT}{Fore.GREEN}✅ Utilisation du token alternatif.{Style.RESET_ALL}")
        return other_token
      else:
        print(f"{Style.BRIGHT}{Fore.YELLOW}❌ Aucun token alternatif fourni. Aucun token ne sera utilisé dans la requête !{Style.RESET_ALL}")
        return None
    # Pas de token utilisé
    else:
      print(f"{Style.BRIGHT}{Fore.YELLOW}❌ Le token ne sera pas utilisé dans la requête...{Style.RESET_ALL}")
      return None

  # Aucun token défini
  print(f"{Style.BRIGHT}{Fore.RED}💣 Aucun token trouvé dans le fichier .env !{Style.RESET_ALL}")
  response = input("🏁 Avez-vous un token à renseigner ? (O/n/e=enregistrer) : ").strip().lower()

  # Enregistrer un nouveau token
  if response == "e":
    set_secret_token(url)
    token = os.getenv("SECRET_TOKEN")

  elif response in ["o", ""]:
    token = input("💬 Entrez votre token : ").strip()

  # Retourner le token ou None si aucun n'est fourni
  return token if token else None



##############################################
##### Fonction pour enregistrer un token #####
##############################################
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
def save_file(temp_parquet_path):
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

      # Copier le fichier temporaire à l'emplacement final
      os.replace(temp_parquet_path, save_path)

      # Extraire le nom de fichier et l'extension
      filename, extension = os.path.splitext(os.path.basename(save_path))

      # Extraire path
      folder_path = os.path.dirname(save_path)

      print(f"{Style.BRIGHT}{Fore.GREEN}📄 {filename}{extension} enregistré sous => {folder_path}/{Style.RESET_ALL}")
    else:
      print(f"{Style.BRIGHT}{Fore.RED}❌ Sauvegarde annulée par l'utilisateur...{Style.RESET_ALL}")

  except PermissionError:
    print(f"{Style.BRIGHT}{Fore.RED}💣 Fichier ouvert, assurez-vous que celui-ci est fermé !{Style.RESET_ALL}")
  except Exception as e:
    print(f"{Style.BRIGHT}{Fore.RED}💣 Erreur lors de la sauvegarde : {e}{Style.RESET_ALL}")

  finally:
    # Nettoyer le fichier temporaire
    if os.path.exists(temp_parquet_path):
      os.remove(temp_parquet_path)



######################################################
##### Fonction pour convertir le JSON en Parquet #####
######################################################
def convert_json_to_parquet(json_data):
  try:
    # Vérifier si le JSON est bien une liste ou un dictionnaire
    if not isinstance(json_data, (list, dict)):
      raise ValueError(f"{Style.BRIGHT}{Fore.RED}💣 Format JSON incorrect !{Style.RESET_ALL}")

    # Convertir le JSON en DataFrame Pandas
    df = pd.DataFrame(json_data)

    # Chemin temporaire
    temp_parquet_path = os.path.join(os.getcwd(), "temp.parquet")

    # Convertir en Parquet + enregistrer temporairement
    df.to_parquet(temp_parquet_path, engine="pyarrow", index=False)

    # Enregistrer le fichier
    save_file(temp_parquet_path)

  except ValueError as ve:
    print(f"{Style.BRIGHT}{Fore.RED}💣 Erreur de conversion : {ve}{Style.RESET_ALL}")
  except Exception as e:
    print(f"{Style.BRIGHT}{Fore.RED}💥 Une erreur s'est produite : {e}{Style.RESET_ALL}")



#####################################################
##### Fonction pour convertir le CSV en Parquet #####
#####################################################
def convert_csv_to_parquet(csv_data):
  try:
    # Si CSV vide ou ne contient que des espaces
    if not csv_data.strip():
      print(f"{Style.BRIGHT}{Fore.RED}⚠️ Ce CSV est vide !{Style.RESET_ALL}")
      return

    # Convertir le CSV en DataFrame Pandas
    df = pd.read_csv(StringIO(csv_data))

    # Chemin temporaire pour le fichier Parquet
    temp_parquet_path = os.path.join(os.getcwd(), "temp.parquet")

    # Convertir en Parquet + enregistrer temporairement
    df.to_parquet(temp_parquet_path, engine="pyarrow", index=False)

    # Enregistrer le fichier
    save_file(temp_parquet_path)

  except ValueError as ve:
    print(f"{Style.BRIGHT}{Fore.RED}💣 Erreur de conversion : {ve}{Style.RESET_ALL}")
  except Exception as e:
    print(f"{Style.BRIGHT}{Fore.RED}💥 Une erreur s'est produite : {e}{Style.RESET_ALL}")



######################################################################
##### Fonction pour ajouter des paramètres de pagination à l'URL #####
######################################################################
def get_paginated_url(url):
  response = input("💬 Souhaitez-vous ajouter des paramètres de pagination à l'URL ? (O/n) : ").strip().lower()

  if response in ["o", ""]:
    results_per_page_value = get_results_per_page()

    return f"{url}?page={page}&per_page={results_per_page_value}"

  # Sinon utiliser l'URL sans paramètres de pagination
  else:
    return url



#################################################################
##### Fonction pour choisir le nombre de résultats par page #####
#################################################################
def get_results_per_page():
  global results_per_page

  # Si résultats par page non défini
  if results_per_page is None:
    try:
      results_per_page_input = input("💬 Combien de résultats par requête souhaitez-vous récupérer ? (max 100) : ").strip()

      # Vérification que l'entrée est un entier
      results_per_page_input = int( results_per_page_input)

      # Dans la limite de 100
      if  results_per_page_input < 1 or  results_per_page_input > 100:
        print(f"{Style.BRIGHT}{Fore.RED}💣 Saisir un nombre entre 1 et 100 !{Style.RESET_ALL}")
        return get_results_per_page()
      else:
        results_per_page = results_per_page_input
        return results_per_page

    except ValueError:
      print(f"{Style.BRIGHT}{Fore.RED}💣 Saisir un nombre entier !{Style.RESET_ALL}")
      return get_results_per_page()

  # Si déjà défini => retourner valeur existante
  return results_per_page



######################################################################
##### Fonction pour extraire les données selon la structure JSON #####
######################################################################
def extract_data_according_json_structure(response_json):
  # Si clé 'data' => on retourne son contenu
  if "data" in response_json:
    return response_json['data']

  # Par défaut on retourne le JSON entier
  return response_json



##############################################################
##### Fonction pour déterminer le type de données reçues #####
##############################################################
def detect_data_format(response):
  try:
    # Tenter de parser les données en JSON
    response.json()
    print(f"{Style.BRIGHT}{Fore.CYAN}📄 Format détecté : JSON{Style.RESET_ALL}")
    return "json"

  except ValueError:
    # Si ce n'est pas du JSON, on continue
    pass

  # Liste des délimiteurs courants pour le CSV
  delimiters = [",", ";", "\t", "|", ":", "#", "/", "\\"]

  # Vérifier si contenu CSV (par la présence de virgules ou délimiteurs)
  content = response.text.strip()
  if content and any(delim in content for delim in delimiters):
    print(f"{Style.BRIGHT}{Fore.CYAN}📄 Format détecté : CSV{Style.RESET_ALL}")
    return "csv"

  # Si aucun format reconnu
  print(f"{Style.BRIGHT}{Fore.RED}💣 Format de données inconnu !{Style.RESET_ALL}")



##################################################
##### Fonction pour choisir l'URL à scrapper #####
##################################################
def api_call(url=None):
  global last_url
  global page

  if url is None:
    invalid_url = False

    while True:
      if not invalid_url:
        prompt_message = "🏁 Entrez l'URL de l'API que vous souhaitez scrapper ('fin' pour quitter) : "
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
        "Accept": "application/json, application/csv"
      }
      # Si token on l'ajoute au header
      if token:
        headers["Authorization"] = f"Bearer {token}"

      # Initialiser la variable avant chaque boucle
      page = 1
      # Si type de données déjà affiché
      data_type_detected = False
      # Liste des résultats JSON
      json_results = []
      # String données CSV
      csv_results = ""

      # Obtenir URL avec ou sans paramètres de pagination
      paginated_url = get_paginated_url(url)

      while True:
        # print(f"🔍 Requête vers l'URL => {paginated_url}")
        response = requests.get(paginated_url, headers=headers)

        # Response
        if response.status_code == 200:
          # Détecter le format des données
          if not data_type_detected:
            try:
              data_format = detect_data_format(response)
              data_type_detected = True
            except ValueError as ve:
              print(f"{Style.BRIGHT}{Fore.RED}💣 Format non détecté : {ve}{Style.RESET_ALL}")
              return

          print(f"{Style.BRIGHT}{Fore.CYAN}👌 Données récupérées... Page : {page}{Style.RESET_ALL}")

          if data_format == "json":
            json_data = response.json()

            # Extraire les données selon la structure
            extracted_data = extract_data_according_json_structure(json_data)

            # Ajouter les résultats de la page actuelle à la liste globale
            json_results.extend(extracted_data)

          elif data_format == "csv":
            # Ajouter le contenu CSV brut dans une chaîne
            csv_results += response.text

          # Vérifier si une autre page existe
          if 'next' in response.links:
            # Incrémentation => page suivante
            page += 1
            # Actualiser URL avec nouveau numéro de page
            paginated_url = f"{url}?page={page}&per_page={get_results_per_page()}"
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
          print(f"{Style.BRIGHT}{Fore.MAGENTA}Échec avec le code de statut {response.status_code} : {response.text}{Style.RESET_ALL}")

      # Appeler les fonctions de convertion en Parquet
      if csv_results:
        convert_csv_to_parquet(csv_results)
      elif json_results:
        convert_json_to_parquet(json_results)

      # Après l'enregistrement du fichier, demander une nouvelle URL
      response = input("🏁 Souhaitez-vous saisir une nouvelle URL ? (O/n) : ").strip().lower()

      if response == "n":
        leave()
      else:
        api_call()

    except ValueError as ve:
      print(ve)
    except requests.exceptions.RequestException as e:
      print(f"{Style.BRIGHT}{Fore.RED}Erreur lors de la requête : {e}{Style.RESET_ALL}")



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
    print(f"{Style.BRIGHT}{Fore.BLUE}👋 Opération interrompue par l'utilisateur. Programme terminé.{Style.RESET_ALL}")
  finally:
    tkInstance.quit()
    tkInstance.destroy()
    sys.exit(0)
