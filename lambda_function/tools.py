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
def get_prompt(markdown_content):
    ENHANCED_STRUCTURED_JSON_PROMPT = f"""Vous êtes un expert en extraction de données de bons de commande de pharmacie français.

Analysez sémantiquement le contenu MARKDOWN et extrayez TOUS les produits présents dans les tableaux.

CONTENU MARKDOWN À ANALYSER :
{markdown_content}

INSTRUCTIONS CRITIQUES POUR EXTRACTION DYNAMIQUE :

1. IDENTIFICATION DE L'ENTREPRISE :
   - Rechercher les en-têtes ## contenant "PHARMACIE", "PARAPHARMACIE"
   - Extraire le nom complet de l'entreprise
   - Localiser l'adresse de facturation ou livraison
   - Extraire le code postal (5 chiffres)

2. INFORMATIONS DE COMMANDE :
   - Date : après "Date de commande :" (format DD/MM/YYYY)
   - Numéro : après "BON DE COMMANDE N°" ou similaire

3. EXTRACTION DYNAMIQUE DE TOUS LES PRODUITS :
   - Identifier TOUS les tableaux de produits avec colonnes : Code Article, Désignation, Qte., etc.
   - Extraire CHAQUE ligne de produit du tableau, MÊME celles avec des valeurs 0,000
   - Le nombre de produits peut varier : 1, 2, 5, 10, 20+ produits
   - INCLURE TOUS LES TYPES : produits principaux, présentoirs, accessoires, échantillons
   - Pour chaque produit trouvé :
     * nom_produit : contenu de la colonne "Désignation" (nettoyer les codes entre parenthèses)
     * quantite : colonne "Qte."
     * prix_unitaire : colonne "P.U.H.T." (convertir virgules en points)
     * remise_pourcentage : colonne "R. (%)" (convertir virgules en points)
     * prix_unitaire_remise : colonne "P.U. rem." (convertir virgules en points)

4. RÈGLES IMPORTANTES :
   - NE PAS limiter le nombre de produits
   - Extraire TOUS les produits trouvés dans le tableau
   - Même les produits gratuits (0,000€) doivent être inclus
   - Convertir virgules → points : 495,60 → 495.60
   - Date au format DD/MM/YYYY : "31/07/2025"

5. STRUCTURE JSON ATTENDUE :
   - Le tableau "produits" doit contenir TOUS les produits trouvés
   - Pas de limite sur le nombre d'éléments dans le tableau
   - Adapter dynamiquement selon le contenu du markdown

TEMPLATE JSON FLEXIBLE À SUIVRE :

{{
  "date_commande": "...",
  "bon_commande": "...",
  "entreprise": {{
    "nom": "...",
    "adresse": "...",
    "code_postal": "..."
  }},
  "produits": [
    {{
      "nom_produit": "...",
      "quantite": ...,
      "prix_unitaire": ...,
      "remise_pourcentage": ...,
      "prix_unitaire_remise": ...
    }}
    // Répéter pour CHAQUE produit trouvé dans le tableau
    // Peut être 1, 2, 5, 10 ou plus de produits
  ],
  "total": ...,
  "metadata": {{
    "pdf_type": "bon_de_commande",
    "structure_detectee": "...",
    "nombre_blocs_analyses": ...,
    "confiance_moyenne": ...,
    "extraction_errors": []
  }}
}}
"""
    return ENHANCED_STRUCTURED_JSON_PROMPT


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
    
    # Génération du prompt strict JSON
    ENHANCED_STRUCTURED_JSON_PROMPT = get_prompt(markdown_content=ocr_text)
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
        
        print("REPONSE")
        print(llm_text)
        
        # Nettoyage strict de la chaîne JSON
        llm_text = llm_text.strip()
        
        # Si le modèle renvoie du texte avant/après le JSON
        if llm_text.startswith("```json") and llm_text.endswith("```"):
            llm_text = llm_text.replace("```json", "").replace("```", "").strip()
        
        # Conversion en dictionnaire Python
        structured_data = json.loads(llm_text)
        return structured_data

    except Exception as e:
        raise RuntimeError(f"Erreur lors de l'extraction JSON avec LLM : {e}")
