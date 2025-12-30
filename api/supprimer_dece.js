import sqlite3 from 'sqlite3';
import { open } from 'sqlite';

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const { id } = req.body;

  if (!id) {
    return res.status(400).json({ error: 'L\'ID est requis' });
  }

  let db;
  try {
    // Ouvrir la base de données
    db = await open({
      filename: './database/data.db',
      driver: sqlite3.Database
    });

    // Supprimer l'enregistrement
    const result = await db.run('DELETE FROM dece WHERE id = ?', [id]);

    if (result.changes === 0) {
      return res.status(404).json({
        success: false,
        error: 'Aucun certificat trouvé avec cet ID'
      });
    }

    res.status(200).json({
      success: true,
      message: 'Certificat de décès supprimé avec succès'
    });
  } catch (error) {
    console.error('Erreur lors de la suppression:', error);
    res.status(500).json({
      success: false,
      error: 'Erreur lors de la suppression: ' + error.message
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