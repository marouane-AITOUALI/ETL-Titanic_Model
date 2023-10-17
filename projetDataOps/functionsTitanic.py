import pandas as pd
import urllib.request
import os
import json

# Fichier : functionsTitanic.py

# Ce fichier contient les fonctions pour mettre en place la pipeline Data
# Les fonctions pour sa création, nettoyage de données ainsi que l'enregistrement
# sous un format précis

# Authors :  {
# Marouane AIT OUALI
# Mohamed Amine Dhaoui
# Achraf Chergui
# Djarlane Innocent Kouassivi EGBLOMASSE
# Jeremy Lambert JAQUES GUSTAVE
# Junior Loic Cedric NGOKOBI
#   }





# request data
def requestUrl(url):
    try:
        data = urllib.request.urlopen(url)
        titanic_data =  pd.read_csv(data)
        return titanic_data
    except Exception as e:
        print(f"Erreur lors des chargement des données: {str(e)}")
        return None


# Fonction pour extraire un modèle de données simplifié au format JSON
def extract_model(data):
    try:
        # Sélectionnez les colonnes nécessaires pour le modèle
        selected_columns = ['Sex', 'Pclass', 'Age', 'Survived', 'Fare', 'Embarked']

        # Créez un dictionnaire pour chaque passager
        passengers = []
        for _, row in data[selected_columns].iterrows():
            passenger = {
                "sex": row['Sex'],
                "class": row['Pclass'],
                "age": row['Age'],
                "survived": row['Survived'],
                "price": row['Fare'],
                "embarked": row['Embarked']
            }
            passengers.append(passenger)

        # Convertissez la liste de passagers en JSON
        
        return passengers
    except Exception as e:
        print(f"Erreur lors de la création du modèle JSON : {str(e)}")
        return None

# Nettoyage/formatage des données
def transform(data):
    
    try:
        # Supprimer les lignes avec des valeurs manquantes ???
        cleaned_data = data.dropna()  
        cleaned_data = data[data['Age'] >= 0]  # Supprimer les valeurs d'âge négatives (le cas échéant)
        return cleaned_data
    except Exception as e:
        print(f"Erreur lors du formatage des données: {str(e)}")
        return None


# Enregistrement des données
def load(dataJson, file_path):
    try:
        outPutFolder = 'output'
        
        if not os.path.exists(outPutFolder):
            os.makedirs(outPutFolder)
        
        newFilePath = os.path.join(outPutFolder, file_path)
        print(newFilePath)
        with open(newFilePath, 'w') as jsonFile:
            json.dump(dataJson, jsonFile, indent=4)
            print(f"Données enregistrées avec succès dans '{file_path}'.")
    except Exception as e:
        print(f"Erreur lors de l'enregistrement des données : {str(e)}")


def data_pipeline(url, json_file_path):
    # Étape 1 : Récupération des données
    raw_data = requestUrl(url)

    if raw_data is not None:
        # Étape 2 : Création du modèle JSON
        cleaned_data = transform(raw_data)
        

        if cleaned_data is not None:
            # Étape 3 : Nettoyage et formatage des données
            model_json = extract_model(cleaned_data)

            if model_json is not None:
                # Étape 4 : Enregistrement des données
                load(model_json, json_file_path)