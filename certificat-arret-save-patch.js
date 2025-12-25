// Patch pour remplacer la fonction de sauvegarde simulée par la vraie fonction
// Copier ce contenu et le coller à la place de la fonction simulée dans certificat.js

try {
    // Remplacer la fonction de sauvegarde simulée dans genererArretTravail
    const sauvegarderFn = async function (message) {
        console.log('🔗 Sauvegarde depuis popup, message:', message);
        
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
        }
    };

    console.log('🔧 Patch de sauvegarde appliqué avec succès');
} catch (error) {
    console.error('❌ Erreur lors de l\'application du patch:', error);
}