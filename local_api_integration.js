// === Intégration avec l'API locale via l'extension Chrome ===

/**
 * Charger les données depuis l'API locale via l'extension
 */
function loadFromLocalAPI() {
    // Vérifier si l'extension est installée
    if (typeof chrome !== 'undefined' && chrome.runtime) {
        // ID de l'extension (à remplacer par l'ID réel de votre extension)
        const extensionId = 'cmcpbphlonkllmnfkhefdjaddokophpb';
        
        // Envoyer un message à l'extension pour récupérer les certificats
        chrome.runtime.sendMessage(
            extensionId,
            {action: 'getArretsTravail'}, // Commencer par les arrêts de travail
            function(response) {
                if (chrome.runtime.lastError) {
                    console.log('Extension non disponible ou erreur:', chrome.runtime.lastError.message || chrome.runtime.lastError);
                    return;
                }
                
                console.log('Réponse de l\'extension:', response);
                
                if (response && response.success) {
                    console.log('Données des certificats récupérées depuis l\'API locale:', response.data);
                    // Mettre à jour l'interface avec les données récupérées
                    updateUIWithLocalCertificatesData(response.data);
                } else {
                    console.log('Erreur lors de la récupération des données des certificats:', response ? response.error : 'Réponse invalide ou vide');
                }
            }
        );
    } else {
        console.log('API Chrome non disponible - pas dans une extension');
    }
}

/**
 * Mettre à jour l'interface utilisateur avec les données des certificats récupérées
 */
function updateUIWithLocalCertificatesData(data) {
    // Mettre à jour l'interface utilisateur avec les données récupérées
    console.log('Mise à jour de l\'UI avec les données des certificats locales:', data);
    
    // Exemple : si vous avez des certificats dans les données, vous pouvez les afficher
    if (data && Array.isArray(data) && data.length > 0) {
        // Par exemple, mettre à jour une liste de certificats si vous en avez une
        // populateCertificatesList(data);
        
        // Ou pré-remplir le formulaire avec le dernier certificat
        const lastCertificate = data[0]; // Prendre le dernier certificat (le plus récent)
        if (lastCertificate) {
            // Pré-remplir les champs du formulaire avec les données du dernier certificat
            if (document.getElementById('patientNomPrenom') && lastCertificate.nom && lastCertificate.prenom) {
                document.getElementById('patientNomPrenom').value = lastCertificate.nom + ' ' + lastCertificate.prenom;
            }
            
            if (document.getElementById('patientDateNaissance') && lastCertificate.date_naissance) {
                document.getElementById('patientDateNaissance').value = lastCertificate.date_naissance;
            }
            
            // Recalculer l'âge si nécessaire
            if (lastCertificate.date_naissance) {
                const age = calculerAge(lastCertificate.date_naissance);
                if (document.getElementById('patientAge')) {
                    document.getElementById('patientAge').value = age;
                }
            }
        }
    }
}

/**
 * Sauvegarder les données actuelles vers l'API locale via l'extension
 */
function saveToLocalAPI(certificateType, certificateData) {
    // Sauvegarder les données actuelles vers l'API locale
    if (typeof chrome !== 'undefined' && chrome.runtime) {
        const extensionId = 'cmcpbphlonkllmnfkhefdjaddokophpb';
        
        // Déterminer l'action en fonction du type de certificat
        let action = '';
        let dataKey = '';
        
        switch(certificateType) {
            case 'arretTravail':
                action = 'addArretTravail';
                dataKey = 'arretData';
                break;
            case 'prolongation':
                action = 'addProlongation';
                dataKey = 'prolongationData';
                break;
            case 'cbv':
                action = 'addCBV';
                dataKey = 'cbvData';
                break;
            case 'antirabique':
                action = 'addAntirabique';
                dataKey = 'antirabiqueData';
                break;
            case 'deces':
                action = 'addDeces';
                dataKey = 'decesData';
                break;
            default:
                console.log('Type de certificat non reconnu:', certificateType);
                return;
        }
        
        // Préparer les données à envoyer
        const requestData = {};
        requestData[dataKey] = certificateData;
        
        // Envoyer un message à l'extension pour sauvegarder les données
        chrome.runtime.sendMessage(
            extensionId,
            {action: action, ...requestData},
            function(response) {
                if (chrome.runtime.lastError) {
                    console.log('Erreur lors de la sauvegarde:', chrome.runtime.lastError.message || chrome.runtime.lastError);
                    return;
                }
                
                console.log('Réponse de sauvegarde:', response);
                
                if (response && response.success) {
                    console.log('Données du certificat sauvegardées avec succès:', response.data);
                    // Ne pas afficher d'alerte pour respecter la préférence utilisateur
                    // Voir mémoire : Suppress Success Alerts on Print Actions
                } else {
                    console.log('Erreur lors de la sauvegarde:', response ? response.error : 'Réponse invalide ou vide');
                    // Afficher une erreur seulement en cas d'échec
                    alert('Erreur lors de la sauvegarde locale du certificat');
                }
            }
        );
    }
}

/**
 * Sauvegarder un certificat d'arrêt de travail via l'API locale Python
 */
async function saveArretTravail() {
    try {
        // Récupérer les données du formulaire
        const nom = document.getElementById('patientNomPrenom').value.split(' ')[0] || '';
        const prenom = document.getElementById('patientNomPrenom').value.split(' ').slice(1).join(' ') || '';
        const medecin = document.getElementById('docteur').value || '';
        const dateCertificat = document.getElementById('dateCertificat').value || '';
        const dateNaissance = document.getElementById('patientDateNaissance').value || '';
        const age = document.getElementById('patientAge').value || '';
        
        // Pour un arrêt de travail, il faut aussi le nombre de jours
        // Vous devrez adapter cela en fonction de votre formulaire spécifique
        const nombreJours = prompt('Nombre de jours d\'arrêt de travail:') || '1';
        
        // Préparer les données du certificat
        const certificateData = {
            nom: nom,
            prenom: prenom,
            medecin: medecin,
            nombre_jours: parseInt(nombreJours),
            date_certificat: dateCertificat,
            date_naissance: dateNaissance || null,
            age: age ? parseInt(age) : null
        };
        
        console.log('Envoi des données à l\'API locale:', certificateData);
        
        // Appeler l'API locale Python
        const response = await fetch('http://localhost:5000/api/ajouter_arret_travail', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(certificateData)
        });
        
        const result = await response.json();
        
        if (result.success) {
            console.log('✅ Arrêt de travail sauvegardé avec succès:', result.message);
            // Afficher un message de succès discret
            showSuccessMessage('Arrêt de travail sauvegardé avec succès');
        } else {
            console.error('❌ Erreur lors de la sauvegarde:', result.error);
            alert('Erreur lors de la sauvegarde: ' + result.error);
        }
        
    } catch (error) {
        console.error('❌ Erreur réseau:', error);
        alert('Erreur de connexion à l\'API locale. Vérifiez que le serveur Python est démarré.');
    }
}

/**
 * Afficher un message de succès discret
 */
function showSuccessMessage(message) {
    // Créer un élément temporaire pour afficher le message
    const successDiv = document.createElement('div');
    successDiv.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        background: #4CAF50;
        color: white;
        padding: 12px 20px;
        border-radius: 6px;
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
        z-index: 10000;
        font-family: Arial, sans-serif;
        font-size: 14px;
    `;
    successDiv.textContent = message;
    
    document.body.appendChild(successDiv);
    
    // Supprimer le message après 3 secondes
    setTimeout(() => {
        if (successDiv.parentNode) {
            successDiv.parentNode.removeChild(successDiv);
        }
    }, 3000);
}

// Charger les données depuis l'API locale lorsque le DOM est prêt
document.addEventListener('DOMContentLoaded', function() {
    // Charger également les données depuis l'API locale
    loadFromLocalAPI();
    
    // Ajouter un écouteur pour le bouton de sauvegarde si présent
    const saveButton = document.getElementById('saveButton');
    if (saveButton) {
        saveButton.addEventListener('click', function() {
            // Sauvegarder les données localement (comme avant)
            saveData();
            
            // Sauvegarder également vers l'API locale
            saveArretTravail();
        });
    }
});