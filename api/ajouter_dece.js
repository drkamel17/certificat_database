import sqlite3 from 'sqlite3';
import { open } from 'sqlite';

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const formData = req.body;

  if (!formData) {
    return res.status(400).json({ error: 'Les données du formulaire sont requises' });
  }

  let db;
  try {
    // Ouvrir la base de données
    db = await open({
      filename: './database/data.db',
      driver: sqlite3.Database
    });

    // Map frontend keys to DB keys (snake_case)
    if (formData.dateDeces) {
      formData.date_deces = formData.dateDeces;
    }
    if (formData.heureDeces) {
      formData.heure_deces = formData.heureDeces;
    }

    // Construire la requête d'insertion dynamiquement
    const columns = [];
    const values = [];
    
    // Liste de tous les champs possibles
    const all_fields = [
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
    ];
    
    for (const field of all_fields) {
      if (field in formData) {
        columns.push(field);
        values.push(formData[field]);
      }
    }
    
    if (columns.length === 0) {
      return res.status(400).json({ error: 'Aucune donnée à insérer' });
    }
    
    // Créer la requête SQL
    const placeholders = columns.map(() => '?').join(', ');
    const columns_str = columns.join(', ');
    
    const query = `
      INSERT INTO dece (${columns_str})
      VALUES (${placeholders})
    `;
    
    const result = await db.run(query, values);
    
    res.status(200).json({
      success: true,
      message: 'Certificat de décès ajouté avec succès',
      id: result.lastID
    });
  } catch (error) {
    console.error('Erreur de base de données:', error);
    res.status(500).json({
      success: false,
      error: 'Erreur de base de données: ' + error.message
    });
  } finally {
    if (db) {
      await db.close();
    }
  }
}

export const config = {
  api: {
    bodyParser: {
      sizeLimit: '10mb'
    }
  }
};