import os
import requests
import sys
import pandas as pd
# import pyarrow.parquet as pq

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



##############################################
##### Fonction pour quitter le programme #####
##############################################
def leave():
  print("👋 Programme terminé.")
  sys.exit(0)



###################################################
##### Fonction pour récupérer le secret token #####
###################################################
def get_secret_token():
  token = os.getenv("SECRET_TOKEN")

  if token == "":
    print("💣 Token vide !")

  if not token:
    print("💣 Aucun token trouvé dans le fichier .env.")
    response = input("🏁 Avez-vous un secret token secret à fournir ? (O/n) : ").strip().lower()

    if response in ["o", ""]:
      token = input("Entrez votre secret token : ").strip()

  return token



####################################################
##### Fonction pour renseigner un secret token #####
####################################################
def set_secret_token(url):
  global last_url
  last_url = url

  while True:
    secret_token = input("💥 Unauthorized request ! Entrez un secret token ('fin' pour quitter) : ").strip()

    if secret_token.lower() == "fin":
      leave()

    if secret_token:
      # Créer le fichier .env uniquement si un token est fourni
      if not os.path.exists(".env"):
        with open(".env", "w") as f:
          f.write("")

      # Enregistrer le token dans le fichier .env
      os.environ["SECRET_TOKEN"] = secret_token
      with open(".env", "a") as f:
        f.write(f"SECRET_TOKEN={secret_token}\n")
      print("✅ Token enregistré dans le fichier .env")

      # Quitter la boucle si un token est saisi
      break

  # Relancer la requête
  api_call(last_url)



########################################################
##### Fonction pour enregistrer le fichier Parquet #####
########################################################
def save_file(df):
  try:
    print("📂 Veuillez sélectionner un emplacement pour sauvegarder le fichier.")
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
      print("\n")
      print(f"📄 {filename}{extension} enregistré sous: {save_path}")
    else:
      print("❌ Action annulée par l'utilisateur. Programme terminé.")

  except PermissionError:
    print("💣 Fichier ouvert, assurez-vous que celui-ci est fermé !")

  except Exception as e:
    print(f"💣 Erreur lors de la sauvegarde : {e}")



######################################################
##### Fonction pour convertir le JSON en Parquet #####
######################################################
def convert_json_to_parquet(json_data):
  try:
    # Vérifier si le JSON est bien une liste ou un dictionnaire
    if not isinstance(json_data, (list, dict)):
      raise ValueError("💣 Les données JSON doivent être une liste ou un dictionnaire !")

    # Convertir le JSON en DataFrame Pandas
    df = pd.DataFrame(json_data)

    # Enregistrer le fichier
    save_file(df)

  except ValueError as ve:
    print(f"💣 Erreur de conversion : {ve}")
  except Exception as e:
    print(f"💥 Une erreur s'est produite : {e}")



##################################################
##### Fonction pour choisir l'URL à scrapper #####
##################################################
def api_call(url=None):
  global last_url

  if url is None:
    invalid_url = False

    while True:
      if not invalid_url:
        prompt_message = "Entrez l'URL de l'API que vous souhaitez scrapper ('fin' pour quitter) : "
      else:
        prompt_message = "Saisir une autre URL ('fin' pour quitter) : "

      # Demander à l'utilisateur de saisir une URL
      url = input(prompt_message).strip()

      if url.lower() == "fin":
        leave()

      if not (url.startswith("https://") or url.startswith("http://")):
        if not invalid_url:
          invalid_url = True
        print("💣 URL invalide !")
        continue
      # Si URL correcte => on sort de la boucle
      break

    try:
      token = get_secret_token()

      # Ajout du header
      headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3+json"
      }

      response = requests.get(url, headers=headers)

      # Response
      if response.status_code == 200:
        print("👌 Données récupérées...")
        json_data = response.json()

        # Appeler la fonction pour convertir en Parquet
        convert_json_to_parquet(json_data)

      elif response.status_code == 401:
        set_secret_token(url)
        return

      else:
        print(f"Échec avec le code de statut {response.status_code} : {response.text}")

    except ValueError as ve:
      print(ve)
    except requests.exceptions.RequestException as e:
      print(f"Erreur lors de l'appel à l'API : {e}")



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
    print(f"\n💥 Opération interrompue par l'utilisateur. Programme terminé.")
  finally:
    tkInstance.quit()
    tkInstance.destroy()
    sys.exit(0)
