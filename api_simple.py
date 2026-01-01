#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import http.server
import socketserver
import json
import sqlite3
import os
import urllib.parse
from datetime import datetime

# Configuration
PORT = 5000
DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'database', 'data.db'))

def init_db():
    """Initialize the database (arrets_travail, prolongation and cbv tables)"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Créer la table pour les arrêts de travail
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS arrets_travail (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nom TEXT NOT NULL,
        prenom TEXT NOT NULL,
        medecin TEXT NOT NULL,
        nombre_jours INTEGER NOT NULL,
        date_certificat DATE NOT NULL,
        date_naissance TEXT,  -- Accepte tout type d'entrée et peut être NULL
        age INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    # Créer la table pour les prolongations d'arrêt de travail
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS prolongation (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nom TEXT NOT NULL,
        prenom TEXT NOT NULL,
        medecin TEXT NOT NULL,
        nombre_jours INTEGER NOT NULL,
        date_certificat DATE NOT NULL,
        date_naissance TEXT,  -- Accepte tout type d'entrée et peut être NULL
        age INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    # Créer la table pour cbv
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS cbv (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nom TEXT NOT NULL,
        prenom TEXT NOT NULL,
        medecin TEXT NOT NULL,
        date_certificat DATE NOT NULL,
        heure TEXT,
        date_naissance TEXT,
        titre TEXT,
        examen TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    # Créer la table pour antirabique
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS antirabique (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nom TEXT,
        prenom TEXT,
        medecin TEXT,
        classe TEXT,
        type_de_vaccin TEXT,
        shema TEXT,
        date_de_certificat DATE,
        date_de_naissance TEXT,
        animal TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    # Créer la table pour les certificats de décès
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS dece (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nom TEXT,
        prenom TEXT,
        dateNaissance TEXT,
        datePresume TEXT,
        wilaya_naissance TEXT,
        sexe TEXT,
        pere TEXT,
        mere TEXT,
        communeNaissance TEXT,
        wilayaResidence TEXT,
        place TEXT,
        placefr TEXT,
        DSG TEXT,
        DECEMAT TEXT,
        DGRO TEXT,
        DACC TEXT,
        DAVO TEXT,
        AGESTATION TEXT,
        IDETER TEXT,
        GM TEXT,
        MN TEXT,
        AGEGEST TEXT,
        POIDNSC TEXT,
        AGEMERE TEXT,
        DPNAT TEXT,
        EMDPNAT TEXT,
        communeResidence TEXT,
        dateDeces TEXT,
        heureDeces TEXT,
        lieuDeces TEXT,
        autresLieuDeces TEXT,
        communeDeces TEXT,
        wilayaDeces TEXT,
        causeDeces TEXT,
        causeDirecte TEXT,
        etatMorbide TEXT,
        natureMort TEXT,
        natureMortAutre TEXT,
        obstacleMedicoLegal TEXT,
        contamination TEXT,
        prothese TEXT,
        POSTOPP2 TEXT,
        CIM1 TEXT,
        CIM2 TEXT,
        CIM3 TEXT,
        CIM4 TEXT,
        CIM5 TEXT,
        nom_ar TEXT,
        prenom_ar TEXT,
        perear TEXT,
        merear TEXT,
        lieu_naissance TEXT,
        conjoint TEXT,
        profession TEXT,
        adresse TEXT,
        date_entree TEXT,
        heure_entree TEXT,
        date_deces TEXT,
        heure_deces TEXT,
        wilaya_deces TEXT,
        medecin TEXT,
        code_p TEXT,
        code_c TEXT,
        code_n TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    conn.commit()
    conn.close()

class APIHandler(http.server.BaseHTTPRequestHandler):
    def _set_headers(self, status_code=200):
        self.send_response(status_code)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
    
    def do_OPTIONS(self):
        self._set_headers(200)
    
    def do_GET(self):
        if self.path == '/api/test':
            self._set_headers(200)
            response = {
                'success': True,
                'message': 'API locale fonctionnelle'
            }
            self.wfile.write(json.dumps(response).encode())
        else:
            self._set_headers(404)
            response = {'error': 'Endpoint non trouvé'}
            self.wfile.write(json.dumps(response).encode())
    
    def do_POST(self):
        if self.path == '/api/ajouter_arret_travail':
            try:
                # Lire le corps de la requête
                content_length = int(self.headers['Content-Length'])
                post_data = self.rfile.read(content_length)
                data = json.loads(post_data.decode('utf-8'))
                
                print(f"Données reçues: {data}")
                
                # Appeler la fonction d'ajout d'arrêt de travail
                success, message = ajouter_arret_travail(
                    nom=data.get('nom', ''),
                    prenom=data.get('prenom', ''),
                    medecin=data.get('medecin', ''),
                    nombre_jours=data.get('nombre_jours', 1),
                    date_certificat=data.get('date_certificat', ''),
                    date_naissance=data.get('date_naissance', None),
                    age=data.get('age', None)
                )
                
                if success:
                    self._set_headers(200)
                    response = {
                        'success': True,
                        'message': message
                    }
                else:
                    self._set_headers(400)
                    response = {
                        'success': False,
                        'error': message
                    }
                
                self.wfile.write(json.dumps(response).encode())
                
            except Exception as e:
                self._set_headers(500)
                response = {
                    'success': False,
                    'error': f'Erreur serveur: {str(e)}'
                }
                self.wfile.write(json.dumps(response).encode())
        elif self.path == '/api/ajouter_prolongation':
            try:
                # Lire le corps de la requête
                content_length = int(self.headers['Content-Length'])
                post_data = self.rfile.read(content_length)
                data = json.loads(post_data.decode('utf-8'))
                
                print(f"Données de prolongation reçues: {data}")
                
                # Appeler la fonction d'ajout de prolongation
                success, message = ajouter_prolongation(
                    nom=data.get('nom', ''),
                    prenom=data.get('prenom', ''),
                    medecin=data.get('medecin', ''),
                    nombre_jours=data.get('nombre_jours', 1),
                    date_certificat=data.get('date_certificat', ''),
                    date_naissance=data.get('date_naissance', None),
                    age=data.get('age', None)
                )
                
                if success:
                    self._set_headers(200)
                    response = {
                        'success': True,
                        'message': message
                    }
                else:
                    self._set_headers(400)
                    response = {
                        'success': False,
                        'error': message
                    }
                
                self.wfile.write(json.dumps(response).encode())
                
            except Exception as e:
                self._set_headers(500)
                response = {
                    'success': False,
                    'error': f'Erreur serveur: {str(e)}'
                }
        elif self.path == "/api/ajouter_cbv":
            try:
                # Lire le corps de la requête
                content_length = int(self.headers["Content-Length"])
                post_data = self.rfile.read(content_length)
                data = json.loads(post_data.decode("utf-8"))
                
                print(f"Données CBV reçues: {data}")
                
                # Appeler la fonction d"ajout de CBV
                success, message = ajouter_cbv(
                    nom=data.get("nom", ""),
                    prenom=data.get("prenom", ""),
                    medecin=data.get("medecin", ""),
                    date_certificat=data.get("date_certificat", ""),
                    heure=data.get("heure", None),
                    date_naissance=data.get("date_naissance", None),
                    titre=data.get("titre", None),
                    examen=data.get("examen", None)
                )
                
                self._set_headers(200)
                response = {
                    'success': success,
                    'message': message
                }
                self.wfile.write(json.dumps(response, ensure_ascii=False).encode('utf-8'))
                
            except Exception as e:
                self._set_headers(500)
                response = {
                    'success': False,
                    'message': f'Erreur serveur: {str(e)}'
                }
                self.wfile.write(json.dumps(response, ensure_ascii=False).encode('utf-8'))
                
        elif self.path == "/api/ajouter_antirabique":
            try:
                # Lire le corps de la requête
                content_length = int(self.headers["Content-Length"])
                post_data = self.rfile.read(content_length)
                data = json.loads(post_data.decode("utf-8"))
                
                print(f"Données antirabique reçues: {data}")
                
                # Appeler la fonction d'ajout antirabique
                success, message = ajouter_antirabique(
                    nom=data.get("nom", ""),
                    prenom=data.get("prenom", ""),
                    medecin=data.get("medecin", ""),
                    classe=data.get("classe", ""),
                    type_de_vaccin=data.get("type_de_vaccin", ""),
                    shema=data.get("shema", ""),
                    date_de_certificat=data.get("date_de_certificat", ""),
                    date_de_naissance=data.get("date_de_naissance", None),
                    animal=data.get("animal", "")
                )
                
                self._set_headers(200)
                response = {
                    'success': success,
                    'message': message
                }
                self.wfile.write(json.dumps(response, ensure_ascii=False).encode('utf-8'))
                
            except Exception as e:
                self._set_headers(500)
                response = {
                    'success': False,
                    'message': f'Erreur serveur: {str(e)}'
                }
                self.wfile.write(json.dumps(response, ensure_ascii=False).encode('utf-8'))
                
        elif self.path == "/api/ajouter_antirabique":
            try:
                # Lire le corps de la requête
                content_length = int(self.headers["Content-Length"])
                post_data = self.rfile.read(content_length)
                data = json.loads(post_data.decode("utf-8"))
                
                print(f"Données antirabique reçues: {data}")
                
                # Appeler la fonction d'ajout antirabique
                success, message = ajouter_antirabique(
                    nom=data.get("nom", ""),
                    prenom=data.get("prenom", ""),
                    medecin=data.get("medecin", ""),
                    classe=data.get("classe", ""),
                    type_de_vaccin=data.get("type_de_vaccin", ""),
                    shema=data.get("shema", ""),
                    date_de_certificat=data.get("date_de_certificat", ""),
                    date_de_naissance=data.get("date_de_naissance", None),
                    animal=data.get("animal", "")
                )
                
                if success:
                    self._set_headers(200)
                    response = {
                        "success": True,
                        "message": message
                    }
                else:
                    self._set_headers(400)
                    response = {
                        "success": False,
                        "error": message
                    }
                
                self.wfile.write(json.dumps(response).encode())
            
            except Exception as e:
                self._set_headers(500)
                response = {
                    'success': False,
                    'error': f'Erreur serveur: {str(e)}'
                }
                self.wfile.write(json.dumps(response).encode())
        elif self.path == "/api/recuperer_donnees":
            try:
                print("Endpoint /api/recuperer_donnees appelé")
                
                # Lire le corps de la requête
                content_length = int(self.headers["Content-Length"])
                post_data = self.rfile.read(content_length)
                data = json.loads(post_data.decode("utf-8"))
                
                print(f"Données de récupération reçues: {data}")
                print(f"Table: {data.get('table', '')}")
                print(f"Date début: {data.get('date_debut', '')}")
                print(f"Date fin: {data.get('date_fin', '')}")
                
                # Appeler la fonction de récupération des données
                result = recuperer_donnees_entre_dates(
                    table=data.get("table", ""),
                    date_debut=data.get("date_debut", ""),
                    date_fin=data.get("date_fin", "")
                )
                
                print(f"Résultat de la récupération: {result}")
                
                if result['ok']:
                    self._set_headers(200)
                    response = {
                        'success': True,
                        'data': result['data'],
                        'total': result['total'],
                        'returned': result['returned']
                    }
                else:
                    self._set_headers(400)
                    response = {
                        'success': False,
                        'error': result['error']
                    }
                
                self.wfile.write(json.dumps(response, ensure_ascii=False).encode('utf-8'))
                
            except Exception as e:
                self._set_headers(500)
                response = {
                    'success': False,
                    'error': f'Erreur serveur: {str(e)}'
                }
                self.wfile.write(json.dumps(response, ensure_ascii=False).encode('utf-8'))
        elif self.path == "/api/modifier_enregistrement":
            try:
                # Lire le corps de la requête
                content_length = int(self.headers["Content-Length"])
                post_data = self.rfile.read(content_length)
                data = json.loads(post_data.decode("utf-8"))
                
                print(f"Données de modification reçues: {data}")
                
                # Appeler la fonction de modification
                result = modifier_enregistrement(
                    table=data.get("table", ""),
                    update_data=data.get("data", {})
                )
                
                if result['ok']:
                    self._set_headers(200)
                    response = {
                        'success': True,
                        'message': 'Enregistrement modifié avec succès'
                    }
                else:
                    self._set_headers(400)
                    response = {
                        'success': False,
                        'error': result['error']
                    }
                
                self.wfile.write(json.dumps(response, ensure_ascii=False).encode('utf-8'))
                
            except Exception as e:
                self._set_headers(500)
                response = {
                    'success': False,
                    'error': f'Erreur serveur: {str(e)}'
                }
                self.wfile.write(json.dumps(response, ensure_ascii=False).encode('utf-8'))
        elif self.path == "/api/ajouter_dece":
            try:
                # Lire le corps de la requête
                content_length = int(self.headers["Content-Length"])
                post_data = self.rfile.read(content_length)
                data = json.loads(post_data.decode("utf-8"))
                
                print(f"Données de décès reçues: {data}")
                
                # Appeler la fonction d'ajout de certificat de décès
                success, message = ajouter_dece(data)
                
                if success:
                    self._set_headers(200)
                    response = {
                        'success': True,
                        'message': message
                    }
                else:
                    self._set_headers(400)
                    response = {
                        'success': False,
                        'error': message
                    }
                
                self.wfile.write(json.dumps(response, ensure_ascii=False).encode('utf-8'))
                
            except Exception as e:
                self._set_headers(500)
                response = {
                    'success': False,
                    'error': f'Erreur serveur: {str(e)}'
                }
                self.wfile.write(json.dumps(response, ensure_ascii=False).encode('utf-8'))
        elif self.path == "/api/modifier_dece":
            try:
                # Lire le corps de la requête
                content_length = int(self.headers["Content-Length"])
                post_data = self.rfile.read(content_length)
                data = json.loads(post_data.decode("utf-8"))
                
                print(f"Données de modification de décès reçues: {data}")
                
                # Appeler la fonction de modification de certificat de décès
                success, message = modifier_dece(data)
                
                if success:
                    self._set_headers(200)
                    response = {
                        'success': True,
                        'message': message
                    }
                else:
                    self._set_headers(400)
                    response = {
                        'success': False,
                        'error': message
                    }
                
                self.wfile.write(json.dumps(response, ensure_ascii=False).encode('utf-8'))
                
            except Exception as e:
                self._set_headers(500)
                response = {
                    'success': False,
                    'error': f'Erreur serveur: {str(e)}'
                }
                self.wfile.write(json.dumps(response, ensure_ascii=False).encode('utf-8'))
        elif self.path == "/api/supprimer_enregistrement":
            try:
                # Lire le corps de la requête
                content_length = int(self.headers["Content-Length"])
                post_data = self.rfile.read(content_length)
                data = json.loads(post_data.decode("utf-8"))
                
                print(f"Données de suppression reçues: {data}")
                
                # Appeler la fonction de suppression
                result = supprimer_enregistrement(
                    table=data.get("table", ""),
                    record_id=data.get("id", 0)
                )
                
                if result['ok']:
                    self._set_headers(200)
                    response = {
                        'success': True,
                        'message': 'Enregistrement supprimé avec succès'
                    }
                else:
                    self._set_headers(400)
                    response = {
                        'success': False,
                        'error': result['error']
                    }
                
                self.wfile.write(json.dumps(response, ensure_ascii=False).encode('utf-8'))
                
            except Exception as e:
                self._set_headers(500)
                response = {
                    'success': False,
                    'error': f'Erreur serveur: {str(e)}'
                }
                self.wfile.write(json.dumps(response, ensure_ascii=False).encode('utf-8'))
        elif self.path == "/api/lister_dece":
            try:
                # Lire le corps de la requête
                content_length = int(self.headers["Content-Length"])
                post_data = self.rfile.read(content_length)
                data = json.loads(post_data.decode("utf-8"))
                
                print(f"Données de listing reçues: {data}")
                
                # Appeler la fonction de listing
                result = lister_dece_par_periode(
                    date_debut=data.get("dateDebut", ""),
                    date_fin=data.get("dateFin", "")
                )
                
                if result['ok']:
                    self._set_headers(200)
                    response = {
                        'success': True,
                        'data': result['data'],
                        'total': result['total'],
                        'returned': result['returned']
                    }
                else:
                    self._set_headers(400)
                    response = {
                        'success': False,
                        'error': result['error']
                    }
                
                self.wfile.write(json.dumps(response, ensure_ascii=False).encode('utf-8'))
                
            except Exception as e:
                self._set_headers(500)
                response = {
                    'success': False,
                    'error': f'Erreur serveur: {str(e)}'
                }
                self.wfile.write(json.dumps(response, ensure_ascii=False).encode('utf-8'))
        elif self.path == "/api/supprimer_dece":
            try:
                # Lire le corps de la requête
                content_length = int(self.headers["Content-Length"])
                post_data = self.rfile.read(content_length)
                data = json.loads(post_data.decode("utf-8"))
                
                print(f"Données de suppression de décès reçues: {data}")
                
                # Appeler la fonction de suppression spécifique
                result = supprimer_dece(
                    record_id=data.get("id", 0)
                )
                
                if result['ok']:
                    self._set_headers(200)
                    response = {
                        'success': True,
                        'message': 'Certificat de décès supprimé avec succès'
                    }
                else:
                    self._set_headers(400)
                    response = {
                        'success': False,
                        'error': result['error']
                    }
                
                self.wfile.write(json.dumps(response, ensure_ascii=False).encode('utf-8'))
                
            except Exception as e:
                self._set_headers(500)
                response = {
                    'success': False,
                    'error': f'Erreur serveur: {str(e)}'
                }
                self.wfile.write(json.dumps(response, ensure_ascii=False).encode('utf-8'))
        else:
            self._set_headers(404)
            response = {'error': 'Endpoint non trouvé'}
            self.wfile.write(json.dumps(response).encode())
    
    def log_message(self, format, *args):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {format % args}")

def ajouter_arret_travail(nom, prenom, medecin, nombre_jours, date_certificat, date_naissance=None, age=None):
    """Ajouter un nouveau arrêt de travail"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Vérifier si un arrêt de travail IDENTIQUE existe déjà (tous les champs identiques)
        cursor.execute('''
            SELECT COUNT(*) FROM arrets_travail 
            WHERE nom = ? AND prenom = ? AND medecin = ? 
            AND nombre_jours = ? AND date_certificat = ? 
            AND COALESCE(date_naissance, '') = COALESCE(?, '')
        ''', (nom, prenom, medecin, nombre_jours, date_certificat, date_naissance))
        
        count = cursor.fetchone()[0]
        
        if count > 0:
            return False, "Un arrêt de travail identique existe déjà (tous les champs sont identiques)"
        
        # Ajouter le nouvel arrêt de travail
        cursor.execute('''
            INSERT INTO arrets_travail 
            (nom, prenom, medecin, nombre_jours, date_certificat, date_naissance, age)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (nom, prenom, medecin, nombre_jours, date_certificat, date_naissance, age))
        
        conn.commit()
        print(f"Arret de travail ajoute: {nom} {prenom} - {nombre_jours} jours")
        return True, "Arrêt de travail ajouté avec succès"
    except Exception as e:
        print(f"Erreur lors de l'ajout: {e}")
        return False, f"Erreur: {str(e)}"
    finally:
        conn.close()

def ajouter_prolongation(nom, prenom, medecin, nombre_jours, date_certificat, date_naissance=None, age=None):
    """Ajouter une nouvelle prolongation d'arrêt de travail"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Vérifier si une prolongation IDENTIQUE existe déjà (tous les champs identiques)
        cursor.execute('''
            SELECT COUNT(*) FROM prolongation 
            WHERE nom = ? AND prenom = ? AND medecin = ? 
            AND nombre_jours = ? AND date_certificat = ? 
            AND COALESCE(date_naissance, '') = COALESCE(?, '')
        ''', (nom, prenom, medecin, nombre_jours, date_certificat, date_naissance))
        
        count = cursor.fetchone()[0]
        
        if count > 0:
            return False, "Une prolongation identique existe déjà (tous les champs sont identiques)"
        
        # Ajouter la nouvelle prolongation
        cursor.execute('''
            INSERT INTO prolongation 
            (nom, prenom, medecin, nombre_jours, date_certificat, date_naissance, age)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (nom, prenom, medecin, nombre_jours, date_certificat, date_naissance, age))
        
        conn.commit()
        print(f"Prolongation ajoutee: {nom} {prenom} - {nombre_jours} jours")
        return True, "Prolongation d'arrêt de travail ajoutée avec succès"
    except Exception as e:
        print(f"Erreur lors de l'ajout de la prolongation: {e}")
        return False, f"Erreur: {str(e)}"
    finally:
        conn.close()

def ajouter_cbv(nom, prenom, medecin, date_certificat, heure=None, date_naissance=None, titre=None, examen=None):
    """Ajouter un nouveau certificat CBV"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Vérifier si un certificat CBV IDENTIQUE existe déjà (tous les champs identiques)
        cursor.execute('''
            SELECT COUNT(*) FROM cbv 
            WHERE nom = ? AND prenom = ? AND medecin = ? 
            AND date_certificat = ? 
            AND COALESCE(heure, '') = COALESCE(?, '')
            AND COALESCE(date_naissance, '') = COALESCE(?, '')
            AND COALESCE(titre, '') = COALESCE(?, '')
            AND COALESCE(examen, '') = COALESCE(?, '')
        ''', (nom, prenom, medecin, date_certificat, heure, date_naissance, titre, examen))
        
        count = cursor.fetchone()[0]
        
        if count > 0:
            return False, "Un certificat CBV identique existe déjà (tous les champs sont identiques)"
        
        # Ajouter le nouveau certificat CBV
        cursor.execute('''
            INSERT INTO cbv 
            (nom, prenom, medecin, date_certificat, heure, date_naissance, titre, examen)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (nom, prenom, medecin, date_certificat, heure, date_naissance, titre, examen))
        
        conn.commit()
        print(f"CBV ajouté: {nom} {prenom} - {titre}")
        return True, "CBV santé ajouté avec succès"
    except Exception as e:
        return False, f"Erreur: {str(e)}"
    finally:
        conn.close()


def ajouter_antirabique(nom, prenom, medecin, classe, type_de_vaccin, shema, date_de_certificat, date_de_naissance=None, animal=None):
    """Ajouter un nouveau certificat antirabique"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Ajouter la colonne heure_creation si elle n'existe pas
        cursor.execute("PRAGMA table_info(antirabique)")
        columns = [column[1] for column in cursor.fetchall()]
        
        if 'heure_creation' not in columns:
            cursor.execute("ALTER TABLE antirabique ADD COLUMN heure_creation TEXT")
            print("Colonne heure_creation ajoutée à la table antirabique")
        # Vérifier si un certificat antirabique existe déjà (sans tenir compte de l'heure)
        cursor.execute('''
            SELECT COUNT(*) FROM antirabique 
            WHERE nom = ? AND prenom = ? AND medecin = ? 
            AND classe = ? AND type_de_vaccin = ? AND shema = ?
            AND date_de_certificat = ? 
            AND COALESCE(date_de_naissance, '') = COALESCE(?, '')
            AND COALESCE(animal, '') = COALESCE(?, '')
        ''', (nom, prenom, medecin, classe, type_de_vaccin, shema, date_de_certificat, date_de_naissance, animal))
        
        count = cursor.fetchone()[0]
        
        if count > 0:
            return False, "Un certificat antirabique identique existe déjà (tous les champs sont identiques)"
        
        # Ajouter le nouveau certificat antirabique
        cursor.execute('''
            INSERT INTO antirabique 
            (nom, prenom, medecin, classe, type_de_vaccin, shema, date_de_certificat, date_de_naissance, animal)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (nom, prenom, medecin, classe, type_de_vaccin, shema, date_de_certificat, date_de_naissance, animal))
        
        conn.commit()
        print(f"Certificat antirabique ajouté: {nom} {prenom} - {classe}")
        return True, "Certificat antirabique ajouté avec succès"
    except Exception as e:
        return False, f"Erreur: {str(e)}"
    finally:
        conn.close()


def recuperer_donnees_entre_dates(table, date_debut, date_fin):
    """Récupérer les données d'une table entre deux dates"""
    print(f"Tentative de connexion à la base de données: {DB_PATH}")
    
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        print("Connexion à la base de données réussie")
    except Exception as e:
        print(f"Erreur de connexion à la base de données: {e}")
        return {'ok': False, 'error': f'Erreur de connexion à la base de données: {str(e)}'}
    
    # Vérifier que la table est valide
    tables_valides = ['arrets_travail', 'prolongation', 'cbv', 'antirabique', 'dece']
    if table not in tables_valides:
        conn.close()
        return {'ok': False, 'error': f'Table non valide. Tables valides: {tables_valides}'}
    
    try:
        # Valider les dates
        datetime.strptime(date_debut, '%Y-%m-%d')
        datetime.strptime(date_fin, '%Y-%m-%d')
    except ValueError:
        conn.close()
        return {'ok': False, 'error': 'Format de date invalide. Utilisez AAAA-MM-JJ.'}
    
    try:
        # Construire la requête selon la table
        if table == 'antirabique':
            date_field = 'date_de_certificat'
        elif table == 'dece':
            date_field = 'date_deces'
        else:
            date_field = 'date_certificat'
        
        # Compter le nombre total de résultats
        cursor.execute(f'''
            SELECT COUNT(*) as total 
            FROM {table} 
            WHERE {date_field} BETWEEN ? AND ?
        ''', (date_debut, date_fin))
        total = cursor.fetchone()['total']
        
        # Récupérer les données
        if table == 'arrets_travail' or table == 'prolongation':
            cursor.execute(f'''
                SELECT 
                    id, nom, prenom, medecin, nombre_jours,
                    {date_field}, date_naissance, age,
                    strftime('%Y-%m-%d %H:%M:%S', created_at) as created_at
                FROM {table} 
                WHERE {date_field} BETWEEN ? AND ?
                ORDER BY {date_field} DESC, nom ASC, prenom ASC
            ''', (date_debut, date_fin))
        elif table == 'cbv':
            cursor.execute(f'''
                SELECT 
                    id, nom, prenom, medecin, {date_field}, heure, date_naissance, titre, examen,
                    strftime('%Y-%m-%d %H:%M:%S', created_at) as created_at
                FROM {table} 
                WHERE {date_field} BETWEEN ? AND ?
                ORDER BY {date_field} DESC, nom ASC, prenom ASC
            ''', (date_debut, date_fin))
        elif table == 'antirabique':
            cursor.execute(f'''
                SELECT 
                    id, nom, prenom, medecin, classe, type_de_vaccin, shema, {date_field}, date_de_naissance, animal,
                    strftime('%Y-%m-%d %H:%M:%S', created_at) as created_at
                FROM {table} 
                WHERE {date_field} BETWEEN ? AND ?
                ORDER BY {date_field} DESC, nom ASC, prenom ASC
            ''', (date_debut, date_fin))
        elif table == 'dece':
            cursor.execute(f'''
                SELECT *
                FROM {table} 
                WHERE {date_field} BETWEEN ? AND ?
                ORDER BY {date_field} DESC, nom ASC, prenom ASC
            ''', (date_debut, date_fin))
        
        results = [dict(row) for row in cursor.fetchall()]
        
        if table == 'dece':
            for cert in results:
                if 'date_deces' in cert:
                    cert['dateDeces'] = cert['date_deces']
                if 'heure_deces' in cert:
                    cert['heureDeces'] = cert['heure_deces']
        conn.close()
        
        return {
            'ok': True,
            'data': results,
            'total': total,
            'returned': len(results)
        }
    except Exception as e:
        conn.close()
        return {'ok': False, 'error': f'Erreur lors de la récupération des données: {str(e)}'}

def modifier_enregistrement(table, update_data):
    """Modifier un enregistrement dans une table"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Vérifier que la table est valide
        tables_valides = ['arrets_travail', 'prolongation', 'cbv', 'antirabique']
        if table not in tables_valides:
            conn.close()
            return {'ok': False, 'error': f'Table non valide. Tables valides: {tables_valides}'}
        
        record_id = update_data.get('id')
        if not record_id:
            conn.close()
            return {'ok': False, 'error': 'ID de l\'enregistrement manquant'}
        
        # Construire la requête de modification selon la table
        if table == 'arrets_travail' or table == 'prolongation':
            cursor.execute('''
                UPDATE arrets_travail 
                SET nom = ?, prenom = ?, medecin = ?, nombre_jours = ?, 
                    date_certificat = ?, date_naissance = ?, age = ?
                WHERE id = ?
            ''' if table == 'arrets_travail' else '''
                UPDATE prolongation 
                SET nom = ?, prenom = ?, medecin = ?, nombre_jours = ?, 
                    date_certificat = ?, date_naissance = ?, age = ?
                WHERE id = ?
            ''', (
                update_data.get('nom'),
                update_data.get('prenom'),
                update_data.get('medecin'),
                update_data.get('nombre_jours'),
                update_data.get('date_certificat'),
                update_data.get('date_naissance'),
                update_data.get('age'),
                record_id
            ))
            
        elif table == 'cbv':
            cursor.execute('''
                UPDATE cbv 
                SET nom = ?, prenom = ?, medecin = ?, date_certificat = ?, 
                    heure = ?, date_naissance = ?, titre = ?, examen = ?
                WHERE id = ?
            ''', (
                update_data.get('nom'),
                update_data.get('prenom'),
                update_data.get('medecin'),
                update_data.get('date_certificat'),
                update_data.get('heure'),
                update_data.get('date_naissance'),
                update_data.get('titre'),
                update_data.get('examen'),
                record_id
            ))
            
        elif table == 'antirabique':
            cursor.execute('''
                UPDATE antirabique 
                SET nom = ?, prenom = ?, medecin = ?, classe = ?, 
                    type_de_vaccin = ?, shema = ?, date_de_certificat = ?, 
                    date_de_naissance = ?, animal = ?
                WHERE id = ?
            ''', (
                update_data.get('nom'),
                update_data.get('prenom'),
                update_data.get('medecin'),
                update_data.get('classe'),
                update_data.get('type_de_vaccin'),
                update_data.get('shema'),
                update_data.get('date_de_certificat'),
                update_data.get('date_de_naissance'),
                update_data.get('animal'),
                record_id
            ))
        
        # Vérifier si la modification a réussi
        if cursor.rowcount == 0:
            conn.close()
            return {'ok': False, 'error': 'Aucun enregistrement trouvé avec cet ID'}
        
        conn.commit()
        conn.close()
        
        return {'ok': True, 'message': 'Enregistrement modifié avec succès'}
        
    except Exception as e:
        conn.close()
        return {'ok': False, 'error': f'Erreur lors de la modification: {str(e)}'}

def ajouter_dece(data):
    """Ajouter un nouveau certificat de décès"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Map frontend keys to DB keys (snake_case)
        if 'dateDeces' in data:
            data['date_deces'] = data['dateDeces']
        if 'heureDeces' in data:
            data['heure_deces'] = data['heureDeces']

        # Construire la requête d'insertion dynamiquement
        columns = []
        values = []
        
        # Liste de tous les champs possibles
        all_fields = [
            'nom', 'prenom', 'dateNaissance', 'datePresume', 'wilaya_naissance', 'sexe',
            'pere', 'mere', 'communeNaissance', 'wilayaResidence', 'place', 'placefr',
            'DSG', 'DECEMAT', 'DGRO', 'DACC', 'DAVO', 'AGESTATION', 'IDETER', 'GM',
            'MN', 'AGEGEST', 'POIDNSC', 'AGEMERE', 'DPNAT', 'EMDPNAT', 'communeResidence',
            'dateDeces', 'heureDeces', 'lieuDeces', 'autresLieuDeces', 'communeDeces',
            'wilayaDeces', 'causeDeces', 'causeDirecte', 'etatMorbide', 'natureMort',
            'natureMortAutre', 'obstacleMedicoLegal', 'contamination', 'prothese',
            'POSTOPP2', 'CIM1', 'CIM2', 'CIM3', 'CIM4', 'CIM5', 'nom_ar', 'prenom_ar',
            'perear', 'merear', 'lieu_naissance', 'conjoint', 'profession', 'adresse',
            'date_entree', 'heure_entree', 'date_deces', 'heure_deces', 'wilaya_deces',
            'medecin', 'code_p', 'code_c', 'code_n'
        ]
        
        for field in all_fields:
            if field in data:
                columns.append(field)
                values.append(data[field])
        
        if not columns:
            return False, "Aucune donnée à insérer"
        
        # Créer la requête SQL
        placeholders = ', '.join(['?' for _ in columns])
        columns_str = ', '.join(columns)
        
        query = f'''
            INSERT INTO dece ({columns_str})
            VALUES ({placeholders})
        '''
        
        cursor.execute(query, values)
        conn.commit()
        return True, "Certificat de décès ajouté avec succès"
    except Exception as e:
        return False, f"Erreur de base de données: {str(e)}"
    finally:
        conn.close()

def modifier_dece(data):
    """Modifier un certificat de décès existant"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Vérifier que l'ID est présent
        if 'id' not in data or not data['id']:
            return False, "ID du certificat manquant"
        
        cert_id = data['id']
        
        # Map frontend keys to DB keys (snake_case)
        if 'dateDeces' in data:
            data['date_deces'] = data['dateDeces']
        if 'heureDeces' in data:
            data['heure_deces'] = data['heureDeces']

        # Construire la requête de mise à jour dynamiquement
        columns = []
        values = []
        
        # Liste de tous les champs possibles (hors ID)
        all_fields = [
            'nom', 'prenom', 'dateNaissance', 'datePresume', 'wilaya_naissance', 'sexe',
            'pere', 'mere', 'communeNaissance', 'wilayaResidence', 'place', 'placefr',
            'DSG', 'DECEMAT', 'DGRO', 'DACC', 'DAVO', 'AGESTATION', 'IDETER', 'GM',
            'MN', 'AGEGEST', 'POIDNSC', 'AGEMERE', 'DPNAT', 'EMDPNAT', 'communeResidence',
            'dateDeces', 'heureDeces', 'lieuDeces', 'autresLieuDeces', 'communeDeces',
            'wilayaDeces', 'causeDeces', 'causeDirecte', 'etatMorbide', 'natureMort',
            'natureMortAutre', 'obstacleMedicoLegal', 'contamination', 'prothese',
            'POSTOPP2', 'CIM1', 'CIM2', 'CIM3', 'CIM4', 'CIM5', 'nom_ar', 'prenom_ar',
            'perear', 'merear', 'lieu_naissance', 'conjoint', 'profession', 'adresse',
            'date_entree', 'heure_entree', 'date_deces', 'heure_deces', 'wilaya_deces',
            'medecin', 'code_p', 'code_c', 'code_n'
        ]
        
        for field in all_fields:
            if field in data and field != 'id':
                columns.append(f"{field} = ?")
                values.append(data[field])
        
        if not columns:
            return False, "Aucune donnée à modifier"
        
        # Ajouter l'ID à la fin des valeurs
        values.append(cert_id)
        
        # Créer la requête SQL
        columns_str = ', '.join(columns)
        
        query = f'''
            UPDATE dece 
            SET {columns_str}
            WHERE id = ?
        '''
        
        cursor.execute(query, values)
        conn.commit()
        
        if cursor.rowcount == 0:
            return False, "Aucun certificat trouvé avec cet ID"
        
        return True, "Certificat de décès modifié avec succès"
    except Exception as e:
        return False, f"Erreur de base de données: {str(e)}"
    finally:
        conn.close()


def lister_dece(limit=20, offset=0):
    """Lister les certificats de décès"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute('SELECT COUNT(*) as total FROM dece')
    total = cursor.fetchone()['total']

    cursor.execute('''
        SELECT *
        FROM dece 
        ORDER BY created_at DESC
        LIMIT ? OFFSET ?
    ''', (limit, offset))

    results = [dict(row) for row in cursor.fetchall()]
    conn.close()

    return {
        'ok': True,
        'data': results,
        'total': total,
        'returned': len(results),
        'has_more': (offset + len(results)) < total,
        'offset': offset,
        'limit': limit
    }

def supprimer_enregistrement(table, record_id):
    """Supprimer un enregistrement d'une table"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Vérifier que la table est valide
        tables_valides = ['arrets_travail', 'prolongation', 'cbv', 'antirabique', 'dece']
        if table not in tables_valides:
            conn.close()
            return {'ok': False, 'error': f'Table non valide. Tables valides: {tables_valides}'}
        
        if not record_id:
            conn.close()
            return {'ok': False, 'error': 'ID de l\'enregistrement manquant'}
        
        # Supprimer l'enregistrement
        cursor.execute(f'DELETE FROM {table} WHERE id = ?', (record_id,))
        
        # Vérifier si la suppression a réussi
        if cursor.rowcount == 0:
            conn.close()
            return {'ok': False, 'error': 'Aucun enregistrement trouvé avec cet ID'}
        
        conn.commit()
        conn.close()
        
        return {'ok': True, 'message': 'Enregistrement supprimé avec succès'}
        
    except Exception as e:
        conn.close()
        return {'ok': False, 'error': f'Erreur lors de la suppression: {str(e)}'}


def supprimer_dece(record_id):
    """Supprimer un certificat de décès"""
    return supprimer_enregistrement('dece', record_id)

def lister_dece_par_periode(date_debut, date_fin):
    """Lister les certificats de décès dans une période donnée"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        # Compter le nombre total de résultats
        cursor.execute('''
            SELECT COUNT(*) as total 
            FROM dece 
            WHERE date_deces BETWEEN ? AND ?
        ''', (date_debut, date_fin))
        total = cursor.fetchone()['total']
        
        # Récupérer les données
        cursor.execute('''
            SELECT *
            FROM dece 
            WHERE date_deces BETWEEN ? AND ?
            ORDER BY date_deces DESC, nom ASC, prenom ASC
        ''', (date_debut, date_fin))
        
        results = [dict(row) for row in cursor.fetchall()]
        
        # Remapper certains champs pour correspondre au format attendu
        for cert in results:
            if 'date_deces' in cert:
                cert['dateDeces'] = cert['date_deces']
            if 'heure_deces' in cert:
                cert['heureDeces'] = cert['heure_deces']
        
        conn.close()
        
        return {
            'ok': True,
            'data': results,
            'total': total,
            'returned': len(results)
        }
    except Exception as e:
        conn.close()
        return {'ok': False, 'error': f'Erreur lors de la récupération des données: {str(e)}'}

def main():
    print(f"Demarrage de l'API locale pour les certificats medicaux...")
    print(f"Disponible sur: http://localhost:{PORT}")
    print(f"Base de donnees: {DB_PATH}")
    print("=" * 50)
    
    # Vérifier si la base de données existe, sinon la créer
    if not os.path.exists(DB_PATH):
        print("Base de données non trouvée, création en cours...")
        init_db()
        print("Base de données créée avec succès!")
    
    with socketserver.TCPServer(("", PORT), APIHandler) as httpd:
        print("Serveur démarré. Appuyez sur Ctrl+C pour arrêter.")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nArrêt du serveur...")

if __name__ == '__main__':
    main()