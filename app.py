import requests
import os
import sys
from dotenv import load_dotenv
from tkinter import Tk
from tkinter.filedialog import askopenfilename, asksaveasfilename



################
##### Load #####
################
# Charger variables d'environnement
load_dotenv()

# Créer instance de Tk
tkInstance = Tk()
tkInstance.withdraw()



###################################################
##### Fonction pour récupérer le secret token #####
###################################################
def get_secret_token():
  token = os.getenv("SECRET_TOKEN")

  if token == "":
    raise ValueError("💣 Token vide !")

  if not token:
    print("💣 Aucun token trouvé dans le fichier .env.")
    response = input("🏁 Avez-vous un secret token secret à fournir ? (O/n) : ").strip().lower()

    if response == "n":
      raise ValueError("💣 Vous devez fournir un token pour continuer !")

    token = input("Entrez votre secret token : ").strip()
    if not token:
      raise ValueError("💣 Token requis pour continuer !")

  return token



##################################################
##### Fonction pour choisir l'URL à scrapper #####
##################################################
def api_call():
  while True:
    # Saisie de l'URL
    url = input("Entrez l'URL de l'API que vous souhaitez scrapper : ").strip()

    # Vérification de l'URL
    if not (url.startswith("https://") or url.startswith("http://")):
      print("💣 URL invalide !")
      continue
    break

  try:
    token = get_secret_token()
    # Saisie du token secret
    token = input("Entrez votre token secret GitHub : ").strip()

    # Ajout des en-têtes avec le token
    headers = {
      "Authorization": f"Bearer {token}",
      "Accept": "application/vnd.github.v3+json"  # Version de l'API GitHub
    }

    # Envoi de la requête GET
    response = requests.get(url, headers=headers)

    # Vérification de la réponse
    if response.status_code == 200:
      print("Succès ! Voici la réponse :")
      print(response.json())
    else:
      print(f"Échec avec le code de statut {response.status_code} :")
      print(response.json())
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
