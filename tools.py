import os
import boto3
import json
import requests
from typing import Dict, Any
from dotenv import load_dotenv






# =========================================
# Clés et endpoint directement définis
# =========================================

# Charger les variables d'environnement.
load_dotenv()

# Récupération depuis l'environnement.
LLM_API_KEY      = os.getenv("LLM_API_KEY")
LLM_API_BASE_URL = os.getenv("LLM_API_BASE_URL")

ENDPOINT = f"{LLM_API_BASE_URL}/chat/completions"
HEADERS = {
    "Authorization": f"Bearer {LLM_API_KEY}",
    "Content-Type": "application/json"
}

# Prompt du LLM.
ENHANCED_STRUCTURED_JSON_PROMPT = """
Vous êtes un expert en extraction de données de bons de commande de pharmacie français.

Vous recevez un texte structuré extrait d'un PDF avec des informations spatiales et de confiance.
Le texte est organisé en PAGES, BLOCS et LIGNES avec leurs positions Y et scores de confiance.
Les éléments alignés horizontalement dans une même ligne forment des colonnes de tableau.

INSTRUCTIONS D'ANALYSE :

1. RECHERCHE DE TABLEAUX :
   - Identifiez les lignes qui forment des tableaux de produits
   - Les lignes avec des éléments séparés par des espaces significatifs représentent des colonnes
   - Les colonnes typiques sont : Code/Désignation, Quantité, Prix unitaire, Remise (%), Prix remisé, Total
   - Recherchez les en-têtes de colonnes pour identifier la structure

2. EXTRACTION DES DONNÉES :
   - Nom de l'entreprise : DOIT contenir "pharmacie", "parapharmacie", "officine" ou termes similaires (insensible à la casse), sinon null
   - Date de commande : format DD/MM/YYYY ou similaire
   - Adresse et code postal de la pharmacie
   - Pour chaque produit dans le tableau :
     * Nom du produit (première colonne après code)
     * Quantité (nombre entier dans colonne dédiée)
     * Prix unitaire (nombre décimal)
     * Remise en pourcentage (si disponible)
     * Prix unitaire remisé (si disponible)

3. TRAITEMENT SPÉCIAL :
   - Produits marqués "(GRATUIT)" ou "GRATUIT" : remise = 100%, prix_unitaire_remise = 0.0
   - Ignorez les lignes de totaux, sous-totaux, en-têtes et pieds de page
   - Utilisez les scores de confiance : privilégiez les lignes avec confiance > 0.7
   - Les codes produit commencent par des parenthèses : (3770010539360)

4. ALIGNEMENT SPATIAL :
   - Les éléments sur la même ligne Y appartiennent à la même ligne de produit
   - Analysez l'espacement entre les mots pour identifier les colonnes
   - Position Y similaire = même ligne de tableau
   - Quantité et prix sont des nombres, nom produit est du texte

5. RÈGLES DE PARSING :
   - Si une ligne contient un code produit (nombre entre parenthèses), c'est une ligne produit
   - La quantité est le premier nombre entier après le nom du produit
   - Le prix unitaire est le nombre décimal (format français avec virgule)
   - Distinguez les unités (gr, ml) qui font partie du nom du quantités numériques

RETOURNEZ UNIQUEMENT le JSON suivant :

{
  "date_commande": "...",
  "entreprise": {
    "nom": "...",
    "adresse": "...",
    "code_postal": "..."
  },
  "produits": [
    {
      "nom_produit": "...",
      "quantite": ...,
      "prix_unitaire": ...,
      "remise_pourcentage": ...,
      "prix_unitaire_remise": ...
    }
  ],
  "total": ...,
  "metadata": {
    "pdf_type": "bon_de_commande",
    "structure_detectee": "...",
    "nombre_blocs_analyses": ...,
    "confiance_moyenne": ...,
    "extraction_errors": []
  }
}

RÈGLES :
- Si une information n'est pas trouvée, utilisez null
- Pour remise_pourcentage : valeur décimale (30% = 30.0)
- Analysez la structure spatiale complète pour reconstituer le tableau
- Privilégiez les lignes avec une haute confiance OCR
- Séparez clairement nom produit (texte) et quantité (nombre entier)
- Répondez uniquement avec le JSON, sans explication
"""

# =========================================
# Connexion AWS
# =========================================
AWS_CONNEXION_CHEMS = [
    "ACCESS_KEY_ID_CHEMS",   
    "SECRET_ACCESS_KEY_CHEMS",
    "REGION_CHEMS"      
]

def connexion_aws(liste_connexion=AWS_CONNEXION_CHEMS):
    try:
        load_dotenv()
        s3_client = boto3.client(
            's3',
            aws_access_key_id     = os.environ.get(liste_connexion[0]),
            aws_secret_access_key = os.environ.get(liste_connexion[1]),
            region_name           = os.environ.get(liste_connexion[2])
        )
        print(f"Connexion AWS réussie (région : {os.environ.get(liste_connexion[2])})")
        return {"status": "success", "client": s3_client}
    except Exception as e:
        print(f"Échec de la connexion AWS : {e}")
        return {"status": "error", "client": None}
      
  
# =========================================
# Récupérer le fichier texte OCR le plus récent.
# =========================================
def get_last_file_ocr(s3_client, bucket, prefix="PDF_OCR/"):
    """
    Récupère le fichier le plus récent dans le dossier S3 PDF_OCR/.
    """
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    if "Contents" not in response:
        raise FileNotFoundError(f"Aucun fichier trouvé dans {prefix}")

    # Trier les fichiers par date de modification (LastModified) décroissante
    sorted_files = sorted(response["Contents"], key=lambda x: x["LastModified"], reverse=True)

    # Récupérer le dernier fichier (le plus récent)
    last_file_key = sorted_files[0]["Key"]
    return last_file_key


# =========================================
# Extraction depuis S3 et appel LLM
# =========================================
def extract_data_from_s3_pdf_ocr(s3_client, bucket_name: str, s3_key: str) -> Dict[str, Any]:
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        ocr_text = response["Body"].read().decode("utf-8")
        print(f"✅ Fichier OCR chargé depuis S3 ({s3_key})")
    except Exception as e:
        raise RuntimeError(f"Erreur lors du téléchargement du fichier S3 : {e}")

    messages = [
        {"role": "system", "content": ENHANCED_STRUCTURED_JSON_PROMPT},
        {"role": "user", "content": ocr_text}
    ]

    payload = {
        "model": "gpt-4o-mini",
        "messages": messages,
        "max_tokens": 1500,
        "temperature": 0.0,
        "top_p": 1,
        "stream": False
    }

    try:
        response = requests.post(ENDPOINT, headers=HEADERS, json=payload, timeout=180)
        response.raise_for_status()
        llm_response = response.json()
        llm_text = llm_response["choices"][0]["message"]["content"]
        structured_data = json.loads(llm_text)
        return structured_data
    except Exception as e:
        raise RuntimeError(f"Erreur lors de l'extraction JSON avec LLM : {e}")