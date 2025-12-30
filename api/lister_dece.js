import sqlite3 from 'sqlite3';
import { open } from 'sqlite';

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const { dateDebut, dateFin } = req.body;

  if (!dateDebut || !dateFin) {
    return res.status(400).json({ error: 'Les dates de début et de fin sont requises' });
  }

  let db;
  try {
    // Ouvrir la base de données
    db = await open({
      filename: './database/data.db',
      driver: sqlite3.Database
    });

    // Compter le nombre total de résultats
    const countResult = await db.get(
      'SELECT COUNT(*) as total FROM dece WHERE date_deces BETWEEN ? AND ?',
      [dateDebut, dateFin]
    );
    const total = countResult.total;

    // Récupérer les données
    const results = await db.all(
      `SELECT *
       FROM dece 
       WHERE date_deces BETWEEN ? AND ?
       ORDER BY date_deces DESC, nom ASC, prenom ASC`,
      [dateDebut, dateFin]
    );

    // Remapper certains champs pour correspondre au format attendu
    const mappedResults = results.map(cert => ({
      ...cert,
      dateDeces: cert.date_deces,
      heureDeces: cert.heure_deces
    }));

    res.status(200).json({
      success: true,
      data: mappedResults,
      total: total,
      returned: mappedResults.length
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