import sqlite3 from 'sqlite3';
import { open } from 'sqlite';

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const { table, date_debut, date_fin } = req.body;

  if (!table || !date_debut || !date_fin) {
    return res.status(400).json({ 
      error: 'La table, la date de début et la date de fin sont requises' 
    });
  }

  let db;
  try {
    // Ouvrir la base de données
    db = await open({
      filename: './database/data.db',
      driver: sqlite3.Database
    });

    // Vérifier que la table est valide
    const tables_valides = ['arrets_travail', 'prolongation', 'cbv', 'antirabique', 'dece'];
    if (!tables_valides.includes(table)) {
      return res.status(400).json({ 
        error: `Table non valide. Tables valides: ${tables_valides.join(', ')}` 
      });
    }

    // Valider les dates
    const dateDebut = new Date(date_debut);
    const dateFin = new Date(date_fin);
    if (isNaN(dateDebut.getTime()) || isNaN(dateFin.getTime())) {
      return res.status(400).json({ 
        error: 'Format de date invalide. Utilisez AAAA-MM-JJ.' 
      });
    }

    // Construire la requête selon la table
    let date_field;
    if (table === 'antirabique') {
      date_field = 'date_de_certificat';
    } else if (table === 'dece') {
      date_field = 'date_deces';
    } else {
      date_field = 'date_certificat';
    }

    // Compter le nombre total de résultats
    const countResult = await db.get(`
      SELECT COUNT(*) as total 
      FROM ${table} 
      WHERE ${date_field} BETWEEN ? AND ?
    `, [date_debut, date_fin]);
    const total = countResult.total;

    // Récupérer les données
    let results;
    if (table === 'arrets_travail' || table === 'prolongation') {
      results = await db.all(`
        SELECT 
          id, nom, prenom, medecin, nombre_jours,
          ${date_field}, date_naissance, age,
          created_at
        FROM ${table} 
        WHERE ${date_field} BETWEEN ? AND ?
        ORDER BY ${date_field} DESC, nom ASC, prenom ASC
      `, [date_debut, date_fin]);
    } else if (table === 'cbv') {
      results = await db.all(`
        SELECT 
          id, nom, prenom, medecin, ${date_field}, heure, date_naissance, titre, examen,
          created_at
        FROM ${table} 
        WHERE ${date_field} BETWEEN ? AND ?
        ORDER BY ${date_field} DESC, nom ASC, prenom ASC
      `, [date_debut, date_fin]);
    } else if (table === 'antirabique') {
      results = await db.all(`
        SELECT 
          id, nom, prenom, medecin, classe, type_de_vaccin, shema, ${date_field}, date_de_naissance, animal,
          created_at
        FROM ${table} 
        WHERE ${date_field} BETWEEN ? AND ?
        ORDER BY ${date_field} DESC, nom ASC, prenom ASC
      `, [date_debut, date_fin]);
    } else if (table === 'dece') {
      results = await db.all(`
        SELECT *
        FROM ${table} 
        WHERE ${date_field} BETWEEN ? AND ?
        ORDER BY ${date_field} DESC, nom ASC, prenom ASC
      `, [date_debut, date_fin]);

      // Remapper certains champs pour correspondre au format attendu
      for (const cert of results) {
        if (cert.date_deces) {
          cert.dateDeces = cert.date_deces;
        }
        if (cert.heure_deces) {
          cert.heureDeces = cert.heure_deces;
        }
      }
    }

    res.status(200).json({
      success: true,
      data: results,
      total: total,
      returned: results.length
    });
  } catch (error) {
    console.error('Erreur lors de la récupération des données:', error);
    res.status(500).json({
      success: false,
      error: 'Erreur lors de la récupération des données: ' + error.message
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