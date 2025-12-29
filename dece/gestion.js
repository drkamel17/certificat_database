// ============================================================================
// Script de gestion des certificats de décès
// ============================================================================

console.log('📋 Script de gestion des certificats de décès chargé');

// Variable pour stocker le certificat sélectionné
let selectedCert = null;

// Initialisation au chargement de la page
document.addEventListener('DOMContentLoaded', function () {
    console.log('✅ DOM chargé');

    // Initialiser les dates
    const today = new Date();
    const firstDayOfMonth = new Date(today.getFullYear(), today.getMonth(), 1);

    const dateDebutInput = document.getElementById('dateDebut');
    const dateFinInput = document.getElementById('dateFin');

    if (dateDebutInput && dateFinInput) {
        dateDebutInput.valueAsDate = firstDayOfMonth;
        dateFinInput.valueAsDate = today;
    }

    // Gestionnaire pour le bouton Rechercher
    const btnRechercher = document.getElementById('btnRechercher');
    if (btnRechercher) {
        btnRechercher.addEventListener('click', rechercherCertificats);
    }

    // Gestionnaire pour le bouton Exporter
    const btnExporter = document.getElementById('btnExporter');
    if (btnExporter) {
        btnExporter.addEventListener('click', exporterCSV);
    }

    // Gestionnaire pour le bouton Imprimer (liste)
    const btnImprimer = document.getElementById('btnImprimer');
    if (btnImprimer) {
        btnImprimer.addEventListener('click', () => window.print());
    }

    // Gestionnaires pour les boutons d'action
    const btnRecuperer = document.getElementById('btnRecuperer');
    if (btnRecuperer) {
        btnRecuperer.addEventListener('click', () => {
            if (selectedCert) recupererCertificat(selectedCert);
        });
    }

    const btnModifier = document.getElementById('btnModifier');
    if (btnModifier) {
        btnModifier.addEventListener('click', () => {
            if (selectedCert) modifierCertificat(selectedCert);
        });
    }

    const btnSupprimer = document.getElementById('btnSupprimer');
    if (btnSupprimer) {
        btnSupprimer.addEventListener('click', () => {
            if (selectedCert) supprimerCertificat(selectedCert);
        });
    }

    // Lancer une recherche initiale
    rechercherCertificats();
});

// Fonction pour rechercher les certificats
async function rechercherCertificats() {
    const dateDebut = document.getElementById('dateDebut').value;
    const dateFin = document.getElementById('dateFin').value;
    const loading = document.getElementById('loading');
    const errorMessage = document.getElementById('errorMessage');
    const periodeLine = document.getElementById('periodeLine');

    // Réinitialiser l'interface
    if (loading) loading.style.display = 'block';
    if (errorMessage) errorMessage.style.display = 'none';
    const tableBody = document.getElementById('tableBody');
    if (tableBody) tableBody.innerHTML = '';
    const stats = document.getElementById('stats');
    if (stats) stats.style.display = 'none';

    // Désactiver la barre d'outils et réinitialiser la sélection
    selectedCert = null;
    updateToolbarState();

    try {
        // Afficher la période
        const dateDebutText = document.getElementById('dateDebutText');
        const dateFinText = document.getElementById('dateFinText');
        if (dateDebutText) dateDebutText.textContent = formatDateForDisplay(dateDebut);
        if (dateFinText) dateFinText.textContent = formatDateForDisplay(dateFin);
        if (periodeLine) periodeLine.style.display = 'block';

        // Appeler le backend via Native Messaging
        const response = await sendNativeMessage({
            action: 'lister_dece',
            dateDebut: dateDebut,
            dateFin: dateFin
        });

        if (loading) loading.style.display = 'none';

        if (response && response.data && Array.isArray(response.data)) {
            afficherResultats(response.data);
        } else if (response && response.error) {
            throw new Error(response.error);
        } else {
            afficherResultats([]);
        }

    } catch (error) {
        if (loading) loading.style.display = 'none';
        if (errorMessage) {
            errorMessage.textContent = `Erreur: ${error.message}`;
            errorMessage.style.display = 'block';
        }
        console.error('Erreur recherche:', error);
    }
}

// Fonction pour envoyer un message à l'application native
function sendNativeMessage(message) {
    return new Promise((resolve, reject) => {
        // Détecter le navigateur
        const isFirefox = typeof browser !== 'undefined';
        const isChrome = typeof chrome !== 'undefined' && !isFirefox;

        if (isFirefox) {
            // Firefox
            try {
                const port = browser.runtime.connectNative("com.daoudi.certificat");
                port.onMessage.addListener((response) => {
                    resolve(response);
                    port.disconnect();
                });
                port.onDisconnect.addListener(() => {
                    if (browser.runtime.lastError) {
                        reject(new Error(browser.runtime.lastError.message));
                    } else {
                        // Si pas de réponse mais déconnecté
                    }
                });
                port.postMessage(message);
            } catch (e) {
                reject(e);
            }
        } else if (isChrome) {
            // Chrome
            try {
                chrome.runtime.sendNativeMessage("com.daoudi.certificat", message, (response) => {
                    if (chrome.runtime.lastError) {
                        reject(new Error(chrome.runtime.lastError.message));
                    } else {
                        resolve(response);
                    }
                });
            } catch (e) {
                reject(e);
            }
        } else {
            reject(new Error("Navigateur non supporté"));
        }
    });
}

// Fonction pour afficher les résultats dans le tableau
function afficherResultats(data) {
    const tableBody = document.getElementById('tableBody');
    const statsDiv = document.getElementById('stats');
    const totalCount = document.getElementById('totalCount');

    if (!tableBody) return;

    // Vider le tableau
    tableBody.innerHTML = '';

    if (!data || data.length === 0) {
        tableBody.innerHTML = `
            <tr>
                <td colspan="7" class="px-6 py-4 text-center text-gray-500">
                    Aucun certificat trouvé pour cette période
                </td>
            </tr>
        `;
        if (statsDiv) statsDiv.style.display = 'none';
        return;
    }

    // Afficher les statistiques
    if (totalCount) totalCount.textContent = data.length;
    if (statsDiv) statsDiv.style.display = 'block';

    // Remplir le tableau
    data.forEach((cert, index) => {
        const row = document.createElement('tr');
        row.className = 'hover:bg-blue-50 cursor-pointer transition-colors duration-150 border-b border-gray-100';
        row.dataset.id = cert.id; // Stocker l'ID pour référence

        row.innerHTML = `
            <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900">${index + 1}</td>
            <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">${cert.nom || ''}</td>
            <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900">${cert.prenom || ''}</td>
            <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900">${formatDateForDisplay(cert.dateDeces)}</td>
            <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900">${cert.heureDeces || ''}</td>
            <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900">${cert.lieuDeces || ''}</td>
            <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900">${cert.medecin || ''}</td>
        `;

        // Gestion du clic pour la sélection
        row.addEventListener('click', () => {
            // Désélectionner les autres lignes
            const rows = tableBody.querySelectorAll('tr');
            rows.forEach(r => {
                r.classList.remove('bg-blue-100', 'ring-2', 'ring-blue-500', 'ring-inset');
            });

            // Sélectionner cette ligne
            row.classList.add('bg-blue-100', 'ring-2', 'ring-blue-500', 'ring-inset');

            // Mettre à jour la variable globale
            selectedCert = cert;

            // Mettre à jour l'état de la barre d'outils
            updateToolbarState();
        });

        tableBody.appendChild(row);
    });
}

// Fonction pour mettre à jour l'état de la barre d'outils
function updateToolbarState() {
    const toolbar = document.getElementById('toolbar');
    const selectedInfo = document.getElementById('selectedInfo');

    if (!toolbar || !selectedInfo) return;

    if (selectedCert) {
        toolbar.classList.remove('opacity-50', 'pointer-events-none');
        selectedInfo.textContent = `Sélectionné : ${selectedCert.nom} ${selectedCert.prenom}`;
        selectedInfo.classList.add('text-blue-600', 'font-medium');
        selectedInfo.classList.remove('text-gray-500', 'italic');
    } else {
        toolbar.classList.add('opacity-50', 'pointer-events-none');
        selectedInfo.textContent = 'Aucune ligne sélectionnée';
        selectedInfo.classList.remove('text-blue-600', 'font-medium');
        selectedInfo.classList.add('text-gray-500', 'italic');
    }
}

// Fonction pour récupérer un certificat (Réimpression)
function recupererCertificat(cert) {
    console.log('📄 Récupération du certificat:', cert);
    localStorage.setItem('certificatToLoad', JSON.stringify(cert));
    openPopup();
}

// Fonction pour modifier un certificat
function modifierCertificat(cert) {
    console.log('✏️ Modification du certificat:', cert);
    // On ajoute un flag 'mode_modification' aux données
    const certWithMode = { ...cert, mode_modification: true };
    localStorage.setItem('certificatToLoad', JSON.stringify(certWithMode));
    openPopup();
}

// Fonction pour supprimer un certificat
async function supprimerCertificat(cert) {
    if (!confirm(`Êtes-vous sûr de vouloir supprimer le certificat de ${cert.nom} ${cert.prenom} ?\nCette action est irréversible.`)) {
        return;
    }

    try {
        const response = await sendNativeMessage({
            action: 'supprimer_dece',
            id: cert.id
        });

        if (response && (response.ok || response.status === 'success')) {
            alert('✅ Certificat supprimé avec succès.');
            // Rafraîchir la liste
            rechercherCertificats();
        } else {
            throw new Error(response.error || response.message || 'Erreur inconnue');
        }
    } catch (error) {
        console.error('Erreur suppression:', error);
        alert(`❌ Erreur lors de la suppression : ${error.message}`);
    }
}

// Fonction utilitaire pour ouvrir popup.html
function openPopup() {
    try {
        if (typeof chrome !== 'undefined' && chrome.runtime && chrome.runtime.getURL) {
            const popupUrl = chrome.runtime.getURL('dece/popup.html');
            chrome.tabs.create({ url: popupUrl });
        } else if (typeof browser !== 'undefined' && browser.runtime && browser.runtime.getURL) {
            const popupUrl = browser.runtime.getURL('dece/popup.html');
            browser.tabs.create({ url: popupUrl });
        } else {
            window.open('popup.html', '_blank');
        }
    } catch (error) {
        console.error('❌ Erreur ouverture popup:', error);
        alert('Impossible d\'ouvrir la fenêtre.');
    }
}

// Fonction pour formater la date (YYYY-MM-DD -> DD/MM/YYYY)
function formatDateForDisplay(dateString) {
    if (!dateString) return '';
    const parts = dateString.split('-');
    if (parts.length !== 3) return dateString;
    return `${parts[2]}/${parts[1]}/${parts[0]}`;
}

// Fonction pour exporter en CSV
function exporterCSV() {
    // Récupérer les dates
    const dateDebut = document.getElementById('dateDebut').value;
    const dateFin = document.getElementById('dateFin').value;

    sendNativeMessage({
        action: 'lister_dece',
        dateDebut: dateDebut,
        dateFin: dateFin
    }).then(data => {
        if (!data || data.length === 0) {
            alert('Aucune donnée à exporter');
            return;
        }

        // Créer le contenu CSV
        const headers = ['ID', 'Nom', 'Prénom', 'Date Naissance', 'Date Décès', 'Heure', 'Lieu', 'Commune', 'Wilaya', 'Médecin'];
        let csvContent = headers.join(';') + '\n';

        data.forEach(cert => {
            const row = [
                cert.id,
                `"${cert.nom || ''}"`,
                `"${cert.prenom || ''}"`,
                cert.dateNaissance,
                cert.dateDeces,
                cert.heureDeces,
                `"${cert.lieuDeces || ''}"`,
                `"${cert.communeDeces || ''}"`,
                `"${cert.wilayaDeces || ''}"`,
                `"${cert.medecin || ''}"`
            ];
            csvContent += row.join(';') + '\n';
        });

        // Créer un blob et télécharger
        const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.setAttribute('href', url);
        link.setAttribute('download', `certificats_deces_${dateDebut}_${dateFin}.csv`);
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    }).catch(err => {
        console.error('Erreur export:', err);
        alert('Erreur lors de l\'exportation CSV');
    });
}
