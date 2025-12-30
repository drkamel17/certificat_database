import sqlite3 from 'sqlite3';
import { open } from 'sqlite';

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const formData = req.body;

  if (!formData || !formData.id) {
    return res.status(400).json({ error: 'Les données du formulaire et l\'ID sont requis' });
  }

  let db;
  try {
    // Ouvrir la base de données
    db = await open({
      filename: './database/data.db',
      driver: sqlite3.Database
    });

    // Vérifier que l'ID est présent
    const certId = formData.id;
    
    // Map frontend keys to DB keys (snake_case)
    if (formData.dateDeces) {
      formData.date_deces = formData.dateDeces;
    }
    if (formData.heureDeces) {
      formData.heure_deces = formData.heureDeces;
    }

    // Construire la requête de mise à jour dynamiquement
    const columns = [];
    const values = [];
    
    // Liste de tous les champs possibles (hors ID)
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
      if (field in formData && field !== 'id') {
        columns.push(`${field} = ?`);
        values.push(formData[field]);
      }
    }
    
    if (columns.length === 0) {
      return res.status(400).json({ error: 'Aucune donnée à modifier' });
    }
    
    // Ajouter l'ID à la fin des valeurs
    values.push(certId);
    
    // Créer la requête SQL
    const columns_str = columns.join(', ');
    
    const query = `
      UPDATE dece 
      SET ${columns_str}
      WHERE id = ?
    `;
    
    const result = await db.run(query, values);
    
    if (result.changes === 0) {
      return res.status(404).json({
        success: false,
        error: 'Aucun certificat trouvé avec cet ID'
      });
    }
    
    res.status(200).json({
      success: true,
      message: 'Certificat de décès modifié avec succès'
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