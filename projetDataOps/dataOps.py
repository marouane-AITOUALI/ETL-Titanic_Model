import numpy as np
import pandas as pd 
from functionsTitanic import *

# Fichier : dataOps.py

# Ce fichier contient la manipulation de données de titanic en utilisant pandas


# Url des données 

url = 'https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv'

# Rajout de la data au Pipeline
# Enregistrement sous forme json 
data_pipeline(url, 'output.json')


# Lecture & Nettoyage de données
titanic_data = transform(requestUrl(url))



# Question 1
femaleUnder18 = titanic_data[(titanic_data['Sex'] == 'female') & (titanic_data['Age'] < 18)]


femaleUnder18Survived = femaleUnder18['Survived']

print("------------------------------------------------------\nQuestion1:")

print(f" Nombre de femmes de moins de 18 ans qui ont survécu : {sum(femaleUnder18Survived)}")

# Question 2
print("------------------------------------------------------\nQuestion2:")
print("Répartition par classe des femmes < 18 and survived : ")

# On prend les femmes < 18 and ayant survécues
survivedFemaleUnder18 = femaleUnder18[femaleUnder18['Survived'] == 1]

# On groupe par Pclasse
survivedByClass = survivedFemaleUnder18.groupby('Pclass')['PassengerId'].count().reset_index()
# Renommage de Colonnes pour mieux comprendre l'output
survivedByClass.columns = ['Pclass', 'Count']

print(survivedByClass)

# Question 3
print("------------------------------------------------------\nQuestion3:")
print("Statut de survie par rapport au port d'embarquement")

# Regrouper les données par le port d'embarquement ('Embarked') et le statut de survie ('Survived')

portSurvivalCounts = titanic_data.groupby(['Embarked', 'Survived'])['PassengerId'].count().unstack()
portSurvivalCounts.columns = ['Not Survived', 'Survived']

portSurvivalCounts = portSurvivalCounts.fillna(0)
print(portSurvivalCounts)

# Question 4
print("------------------------------------------------------\nQuestion4:")
print("La répartition des passagers par âge et par sexe: ")
#   Répartition par âge et par sexe des passagers du navire

# Créer une table croisée dynamique pour la répartition par sexe et âge
pivot_table = pd.pivot_table(titanic_data, values='PassengerId', index='Sex', columns=pd.cut(titanic_data['Age'], bins=[0, 18, 30, 50, 80]), aggfunc='count')

# Renommer les colonnes pour plus de clarté
pivot_table.columns = ['Moins de 18', '18-30 ans', '30-50 ans', '50 ans et plus']

# Afficher la table croisée dynamique
print(pivot_table)












