// Script pour remplacer la fonction de sauvegarde simulée
// Exécuter: node fix-sauvegarde-arret.js

const fs = require('fs');

// Lire le fichier certificat.js
const filePath = 'certificat.js';
let content = fs.readFileSync(filePath, 'utf8');

// Fonction de remplacement
const oldFunction = `            // Simuler une sauvegarde réussie pour le moment
            // TODO: Implémenter la vraie logique de sauvegarde en base de données
            return new Promise((resolve) => {
                setTimeout(() => {
                    console.log('✅ Sauvegarde simulée réussie');
                    resolve({ ok: true, message: 'Arrêt de travail sauvegardé avec succès' });
                }, 1000);
            });`;

const newFunction = `            // Implémentation de la vraie logique de sauvegarde en base de données
            try {
                // Utiliser l'API locale via l'extension
                if (typeof chrome !== 'undefined' && chrome.runtime) {
                    const extensionId = 'cmcpbphlonkllmnfkhefdjaddokophpb';
                    
                    return new Promise((resolve, reject) => {
                        const requestData = {
                            action: 'addArretTravail',
                            arretData: {
                                nom: message.nom,
                                prenom: message.prenom,
                                medecin: message.medecin,
                                nombre_jours: message.nombre_jours,
                                date_certificat: message.date_certificat,
                                date_naissance: message.date_naissance
                            }
                        };
                        
                        chrome.runtime.sendMessage(
                            extensionId,
                            requestData,
                            function(response) {
                                if (chrome.runtime.lastError) {
                                    console.error('Erreur Chrome runtime:', chrome.runtime.lastError);
                                    reject(new Error(chrome.runtime.lastError.message));
                                    return;
                                }
                               
                                if (response && response.success) {
                                    console.log('✅ Arrêt de travail sauvegardé avec succès via API locale');
                                    resolve({ ok: true, message: 'Arrêt de travail sauvegardé avec succès' });
                                } else {
                                    const errorMsg = response ? response.error : 'Réponse invalide';
                                    console.error('❌ Erreur lors de la sauvegarde:', errorMsg);
                                    reject(new Error(errorMsg));
                                }
                            }
                        );
                    });
                } else if (typeof browser !== 'undefined' && browser.runtime && browser.runtime.sendMessage) {
                    // Alternative pour Firefox
                    const extensionId = 'cmcpbphlonkllmnfkhefdjaddokophpb';
                    
                    const requestData = {
                        action: 'addArretTravail',
                        arretData: {
                            nom: message.nom,
                            prenom: message.prenom,
                            medecin: message.medecin,
                            nombre_jours: message.nombre_jours,
                            date_certificat: message.date_certificat,
                            date_naissance: message.date_naissance
                        }
                    };
                    
                    const response = await browser.runtime.sendMessage(extensionId, requestData);
                    
                    if (response && response.success) {
                        console.log('✅ Arrêt de travail sauvegardé avec succès via API locale (Firefox)');
                        return { ok: true, message: 'Arrêt de travail sauvegardé avec succès' };
                    } else {
                        const errorMsg = response ? response.error : 'Réponse invalide';
                        throw new Error(errorMsg);
                    }
                } else {
                    throw new Error('API Chrome/Firefox non disponible');
                }
            } catch (error) {
                console.error('❌ Erreur lors de la sauvegarde:', error);
                throw error;
            }`;

// Remplacer la fonction
const newContent = content.replace(oldFunction, newFunction);

if (content !== newContent) {
    fs.writeFileSync(filePath, newContent, 'utf8');
    console.log('✅ Fonction de sauvegarde remplacée avec succès!');
} else {
    console.log('❌ Ancienne fonction non trouvée, peut-être déjà remplacée');
}