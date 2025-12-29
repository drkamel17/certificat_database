// PDF-lib is loaded as a global object
const PDFLib = window.PDFLib;

// Ensure PDFLib is properly loaded
if (!PDFLib) {
    console.error('PDFLib failed to load');
    throw new Error('PDFLib is not available');
}

// Initialize PDFLib
const PDFDocument = PDFLib.PDFDocument;
const StandardFonts = PDFLib.StandardFonts;
const rgb = PDFLib.rgb;

// ============================================================================
// ============================================================================
// Remplissage automatique du formulaire depuis la gestion
// ============================================================================

// Variable globale pour stocker l'ID du certificat en cours de modification
let currentCertificateId = null;

document.addEventListener('DOMContentLoaded', function () {
    console.log('📋 Vérification des données à charger...');

    // Gestion de la visibilité des boutons selon le contexte
    const certificatData = localStorage.getItem('certificatToLoad');
    const btnNouveau = document.getElementById('submitnew');      // Bouton Création (Bas)
    const btnReimpression = document.getElementById('submitnew-top'); // Bouton Réimpression (Haut)
    const btnModifier = document.getElementById('submit-update'); // Bouton Modification (Nouveau)

    if (certificatData) {
        try {
            const cert = JSON.parse(certificatData);

            // Stocker l'ID globalement AVANT de supprimer le localStorage
            if (cert.id) {
                currentCertificateId = cert.id;
                console.log('📌 ID du certificat stocké:', currentCertificateId);
            }

            if (cert.mode_modification) {
                // CAS 3 : Mode MODIFICATION
                console.log('✏️ Mode MODIFICATION activé pour ID:', currentCertificateId);

                if (btnNouveau) btnNouveau.style.display = 'none';
                if (btnReimpression) btnReimpression.style.display = 'none';
                if (btnModifier) {
                    btnModifier.style.display = 'block';
                    btnModifier.style.backgroundColor = '#eab308'; // yellow-500
                    btnModifier.style.color = 'white';
                }
            } else {
                // CAS 1 : Ouverture depuis la GESTION (Données présentes)
                // -> On veut Réimprimer (Haut visible) mais pas Créer de doublon (Bas caché)
                if (btnNouveau) btnNouveau.style.display = 'none';
                if (btnModifier) btnModifier.style.display = 'none';
                if (btnReimpression) {
                    btnReimpression.style.display = 'block';
                    btnReimpression.textContent = "Réimprimer le certificat";
                    // Utilisation de styles inline pour garantir la couleur si la classe Tailwind n'est pas chargée
                    btnReimpression.style.backgroundColor = '#7e22ce'; // purple-700
                    btnReimpression.style.color = 'white';
                    btnReimpression.className = "px-4 py-2 rounded hover:bg-purple-800 transition shadow-lg font-bold";
                }
            }

            setTimeout(() => {
                if (typeof remplirFormulaire === 'function') {
                    remplirFormulaire(cert);
                } else {
                    console.error('❌ Fonction remplirFormulaire non définie !');
                }
                localStorage.removeItem('certificatToLoad');
            }, 500);
        } catch (error) {
            console.error('❌ Erreur lors du chargement du certificat:', error);
            localStorage.removeItem('certificatToLoad');
        }
    } else {
        // CAS 2 : Ouverture pour NOUVEAU certificat (Pas de données)
        // -> On veut Créer (Bas visible) et le bouton Haut peut être caché ou servir d'autre chose
        // Selon votre demande : "submitnew-top sera invisible et submitnew visible"

        if (btnNouveau) btnNouveau.style.display = 'block';
        if (btnReimpression) btnReimpression.style.display = 'none';
        if (btnModifier) btnModifier.style.display = 'none';

        console.log('🆕 Mode CRÉATION activé');
    }
});

// Fonction pour remplir le formulaire avec les données d'un certificat
function remplirFormulaire(cert) {
    console.log('🔄 DÉBUT Remplissage du formulaire avec:', cert);

    // Helper pour remplir un champ s'il existe
    const setVal = (id, val) => {
        const el = document.getElementById(id);
        if (el) {
            if (val !== undefined && val !== null) {
                console.log(`✅ Remplissage de ${id} avec "${val}"`);
                el.value = val;
                el.dispatchEvent(new Event('change'));
                el.dispatchEvent(new Event('input'));
            }
        }
    };

    // Helper pour cocher un radio button
    const setRadio = (name, val) => {
        if (!val) return;
        const radios = document.getElementsByName(name);
        for (let i = 0; i < radios.length; i++) {
            if (radios[i].value === val) {
                radios[i].checked = true;
                break;
            }
        }
    };

    try {
        setVal('NOM', cert.nom);
        setVal('PRENOM', cert.prenom);
        setVal('DATENS', cert.dateNaissance);
        setVal('SEXE', cert.sexe);
        setVal('FILSDE', cert.pere);
        setVal('ETDE', cert.mere);
        setVal('WILAYAN', cert.wilaya_naissance);
        setVal('COMMUNEN', cert.communeNaissance);
        setVal('WILAYAR', cert.wilayaResidence);
        setVal('COMMUNER', cert.communeResidence);
        setVal('ADRESSE', cert.adresse);
        setVal('DINS', cert.dateDeces);
        setVal('HINS', cert.heureDeces);
        setRadio('LD', cert.lieuDeces);
        setVal('COMMUNED', cert.communeDeces);
        setVal('WILAYAD', cert.wilayaDeces);
        setVal('MEDECIN', cert.medecin);
        setRadio('CD', cert.causeDeces);
        setVal('CIM1', cert.CIM1);
        setVal('CIM2', cert.CIM2);
        setVal('CIM3', cert.CIM3);
        setVal('CIM4', cert.CIM4);
        setVal('CIM5', cert.CIM5);
        setVal('PROFESSION', cert.profession);
        setVal('CONJOINT', cert.conjoint);

        console.log('✅ FIN Remplissage du formulaire terminé');
    } catch (e) {
        console.error('❌ CRASH dans remplirFormulaire:', e);
    }
}


/// Initialisation des onglets au chargement
function setupTabs() {
    // Masquer tous les contenus sauf le premier
    document.querySelectorAll('.tab-pane').forEach((content, index) => {
        if (index === 0) {
            content.classList.remove('hidden');
        } else {
            content.classList.add('hidden');
        }
    });

    // Gestionnaire d'événements pour les boutons next-tab et prev-tab
    const nextTabButton = document.getElementById('next-tab');
    const prevTabButton = document.getElementById('prev-tab');

    if (nextTabButton && prevTabButton) {
        nextTabButton.addEventListener('click', () => {
            const currentTab = document.querySelector('.tab-pane:not(.hidden)');
            if (currentTab) {
                const nextTab = currentTab.nextElementSibling;
                if (nextTab && nextTab.classList.contains('tab-pane')) {
                    currentTab.classList.add('hidden');
                    nextTab.classList.remove('hidden');

                    // Mettre à jour l'état des boutons
                    prevTabButton.disabled = false;
                    prevTabButton.classList.remove('opacity-50');

                    if (!nextTab.nextElementSibling || !nextTab.nextElementSibling.classList.contains('tab-pane')) {
                        nextTabButton.disabled = true;
                        nextTabButton.classList.add('opacity-50');
                    }
                }
            }
        });

        prevTabButton.addEventListener('click', () => {
            const currentTab = document.querySelector('.tab-pane:not(.hidden)');
            if (currentTab) {
                const prevTab = currentTab.previousElementSibling;
                if (prevTab && prevTab.classList.contains('tab-pane')) {
                    currentTab.classList.add('hidden');
                    prevTab.classList.remove('hidden');

                    // Mettre à jour l'état des boutons
                    nextTabButton.disabled = false;
                    nextTabButton.classList.remove('opacity-50');

                    if (!prevTab.previousElementSibling || !prevTab.previousElementSibling.classList.contains('tab-pane')) {
                        prevTabButton.disabled = true;
                        prevTabButton.classList.add('opacity-50');
                    }
                }
            }
        });

        // Initialiser l'état des boutons
        prevTabButton.disabled = true;
        prevTabButton.classList.add('opacity-50');

        const firstTab = document.querySelector('.tab-pane');
        if (firstTab && (!firstTab.nextElementSibling || !firstTab.nextElementSibling.classList.contains('tab-pane'))) {
            nextTabButton.disabled = true;
            nextTabButton.classList.add('opacity-50');
        }
    }

    // Gestionnaire d'événements pour le bouton d'impression
    if (document.getElementById('print-button')) {
        document.getElementById('print-button').addEventListener('click', async (e) => {
            await generateCertificate(e);
        });
    }

    // Gestionnaire d'événements pour le bouton de gestion
    if (document.getElementById('gestion-button')) {
        document.getElementById('gestion-button').addEventListener('click', function () {
            // Ouvrir l'interface de gestion dans un nouvel onglet
            const gestionUrl = chrome.runtime.getURL('dece/gestion.html');
            chrome.tabs.create({ url: gestionUrl });
        });
    }


    // Fonction utilitaire pour le téléchargement
    async function downloadPdf(blob, filename) {
        const url = URL.createObjectURL(blob);
        console.log('Tentative de téléchargement pour:', filename);

        try {
            // Vérifier si nous sommes dans un contexte d'extension
            const isExtension = (typeof browser !== 'undefined' && browser.downloads) ||
                (typeof chrome !== 'undefined' && chrome.downloads);

            console.log('Contexte extension détecté:', isExtension);

            if (typeof browser !== 'undefined' && browser.downloads) {
                console.log('Utilisation de browser.downloads avec saveAs: true');
                await browser.downloads.download({
                    url: url,
                    filename: filename,
                    saveAs: true
                });
            } else if (typeof chrome !== 'undefined' && chrome.downloads) {
                console.log('Utilisation de chrome.downloads avec saveAs: true');
                chrome.downloads.download({
                    url: url,
                    filename: filename,
                    saveAs: true
                });
            } else {
                console.log('API downloads non disponible, utilisation méthode <a>');
                const a = document.createElement('a');
                a.href = url;
                a.download = filename;
                document.body.appendChild(a);
                a.click();
                document.body.removeChild(a);
            }
        } catch (e) {
            console.error('ERREUR lors du téléchargement:', e);
            // Fallback en cas d'erreur
            const a = document.createElement('a');
            a.href = url;
            a.download = filename;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
        }

        setTimeout(() => URL.revokeObjectURL(url), 60000);
    }

// Template HTML intégré pour éviter les problèmes CORS
const certificateHtmlTemplate = `<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Certificat de Décès</title>
    <style>
        @media print {
            body {
                margin: 0;
                padding: 0;
            }
            .no-print {
                display: none !important;
            }
        }
        
        body {
            font-family: Arial, sans-serif;
            line-height: 1.6;
            margin: 20px;
            padding: 20px;
            background-color: #fff;
        }
        
        .certificate {
            max-width: 800px;
            margin: 0 auto;
            border: 2px solid #000;
            padding: 30px;
            background-color: #fff;
        }
        
        .header {
            text-align: center;
            margin-bottom: 30px;
            border-bottom: 2px solid #000;
            padding-bottom: 20px;
        }
        
        .header h1 {
            font-size: 24px;
            font-weight: bold;
            margin: 10px 0;
        }
        
        .header p {
            font-size: 14px;
            margin: 5px 0;
        }
        
        .content {
            margin: 20px 0;
        }
        
        .section {
            margin: 20px 0;
        }
        
        .section h3 {
            font-size: 16px;
            font-weight: bold;
            margin-bottom: 10px;
            border-bottom: 1px solid #ccc;
            padding-bottom: 5px;
        }
        
        .info-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 15px;
            margin: 10px 0;
        }
        
        .info-item {
            margin: 5px 0;
        }
        
        .info-item strong {
            display: inline-block;
            min-width: 150px;
        }
        
        .signature-area {
            margin-top: 50px;
            text-align: right;
        }
        
        .signature-line {
            display: inline-block;
            width: 200px;
            border-bottom: 1px solid #000;
            margin: 0 10px;
        }
        
        .print-button {
            position: fixed;
            top: 20px;
            right: 20px;
            padding: 10px 20px;
            background-color: #007bff;
            color: white;
            border: none;
            border-radius: 5px;
            cursor: pointer;
            font-size: 16px;
            z-index: 1000;
        }
        
        .print-button:hover {
            background-color: #0056b3;
        }
        
        @media print {
            .certificate {
                border: none;
                box-shadow: none;
            }
        }
    </style>
</head>
<body>
    <button class="print-button no-print" onclick="window.print()">Imprimer</button>
    
    <div class="certificate">
        <div class="header">
            <h1>CERTIFICAT MÉDICAL DE DÉCÈS</h1>
            <p>RÉPUBLIQUE ALGÉRIENNE DÉMOCRATIQUE ET POPULAIRE</p>
            <p>MINISTÈRE DE LA SANTÉ, DE LA POPULATION ET DE LA RÉFORME HOSPITALIÈRE</p>
        </div>
        
        <div class="content">
            <div class="section">
                <h3>INFORMATIONS SUR LE DÉFUNT</h3>
                <div class="info-grid">
                    <div class="info-item">
                        <strong>Nom :</strong> {{NOM}}
                    </div>
                    <div class="info-item">
                        <strong>Prénom :</strong> {{PRENOM}}
                    </div>
                    <div class="info-item">
                        <strong>Date de naissance :</strong> {{DATE_NAISSANCE}}
                    </div>
                    <div class="info-item">
                        <strong>Sexe :</strong> {{SEXE}}
                    </div>
                    <div class="info-item">
                        <strong>Âge :</strong> {{AGE}}
                    </div>
                    <div class="info-item">
                        <strong>Fils de :</strong> {{PERE}}
                    </div>
                    <div class="info-item">
                        <strong>Et de :</strong> {{MERE}}
                    </div>
                    <div class="info-item">
                        <strong>Lieu de naissance :</strong> {{COMMUNE_NAISSANCE}}, {{WILAYA_NAISSANCE}}
                    </div>
                </div>
            </div>
            
            <div class="section">
                <h3>INFORMATIONS SUR LE DÉCÈS</h3>
                <div class="info-grid">
                    <div class="info-item">
                        <strong>Date du décès :</strong> {{DATE_DECES}}
                    </div>
                    <div class="info-item">
                        <strong>Heure du décès :</strong> {{HEURE_DECES}}
                    </div>
                    <div class="info-item">
                        <strong>Lieu du décès :</strong> {{COMMUNE_DECES}}, {{WILAYA_DECES}}
                    </div>
                    <div class="info-item">
                        <strong>Lieu de résidence :</strong> {{COMMUNE_RESIDENCE}}, {{WILAYA_RESIDENCE}}
                    </div>
                    <div class="info-item">
                        <strong>Adresse :</strong> {{ADRESSE}}
                    </div>
                    <div class="info-item">
                        <strong>Lieu de signature :</strong> {{PLACEFR}} ({{PLACE}})
                    </div>
                </div>
            </div>
            
            <div class="section">
                <h3>DÉCLARATION MÉDICALE</h3>
                <p>Le soussigné, Dr {{MEDECIN}}, certifie que le décès de la personne désignée ci-dessus, 
                   survenu le {{DATE_DECES}} à {{HEURE_DECES}}, est réel et constant.</p>
                <p>Cause du décès : {{LIEU_DECES}}</p>
            </div>
        </div>
        
        <div class="signature-area">
            <p>Fait à {{PLACEFR}}, le {{DSG}}</p>
            <p>
                Signature du médecin :
                <span class="signature-line"></span>
            </p>
            <p style="margin-top: 30px;">
                Cachet et signature :
                <span class="signature-line"></span>
            </p>
        </div>
    </div>
    
    <script>
        // Auto-impression après chargement
        window.addEventListener('load', function() {
            setTimeout(function() {
                window.print();
            }, 1000);
        });
    </script>
</body>
</html>`;

// Gestionnaire d'événements pour le bouton submitnew
    if (document.getElementById('submitnew')) {
        document.getElementById('submitnew').addEventListener('click', async (e) => {
            try {
                console.log('Début de la génération du certificat HTML');
                const formData = collectFormData();
                console.log('Données du formulaire collectées:', formData);

                // Sauvegarder les données dans localStorage pour la page decesv.html
                localStorage.setItem('certificateData', JSON.stringify(formData));
                console.log('✅ Données sauvegardées dans localStorage');

                // Ouvrir la page decesv.html dans un nouvel onglet
                const certificateUrl = 'decesv.html';
                console.log('📂 Ouverture de la page:', certificateUrl);
                
                // Essayer différentes méthodes pour ouvrir la page
                let opened = false;
                
                // Méthode 1: window.open avec URL relative
                try {
                    const newWindow = window.open(certificateUrl, '_blank');
                    if (newWindow && !newWindow.closed) {
                        console.log('✅ Page ouverte avec window.open');
                        opened = true;
                    }
                } catch (e) {
                    console.log('❌ window.open a échoué:', e.message);
                }
                
                // Méthode 2: URL absolue si la première méthode échoue
                if (!opened) {
                    try {
                        const absoluteUrl = new URL(certificateUrl, window.location.href).href;
                        window.open(absoluteUrl, '_blank');
                        console.log('✅ Page ouverte avec URL absolue');
                        opened = true;
                    } catch (e) {
                        console.log('❌ URL absolue a échoué:', e.message);
                    }
                }
                
                // Méthode 3: construction manuelle de l'URL
                if (!opened) {
                    const currentPath = window.location.pathname;
                    const directory = currentPath.substring(0, currentPath.lastIndexOf('/') + 1);
                    const fullUrl = window.location.origin + directory + certificateUrl;
                    window.open(fullUrl, '_blank');
                    console.log('✅ Page ouverte avec URL construite manuellement');
                }

                // Vérifier si nous modifions un certificat existant
                const certificatData = localStorage.getItem('certificatToLoad');
                if (certificatData) {
                    // Modification d'un certificat existant
                    console.log('💾 Modification du certificat dans la base de données...');
                    await modifierCertificatDeces(formData);
                } else {
                    // Création d'un nouveau certificat
                    console.log('💾 Enregistrement dans la base de données...');
                    await sauvegarderCertificatDeces(formData);
                }

                console.log('✅ Opération terminée avec succès');

            } catch (error) {
                console.error('Erreur détaillée:', error);
                alert('Une erreur est survenue lors de la génération du certificat: ' + error.message);
            }
        });
    }


// Gestionnaire d'événements pour le bouton submitnew-top
    if (document.getElementById('submitnew-top')) {
        document.getElementById('submitnew-top').addEventListener('click', async (e) => {
            try {
                console.log('Début de la génération du certificat HTML (réimpression)');
                const formData = collectFormData();
                console.log('Données du formulaire collectées:', formData);

                // Sauvegarder les données dans localStorage pour la page decesv.html
                localStorage.setItem('certificateData', JSON.stringify(formData));
                console.log('✅ Données sauvegardées dans localStorage');

                // Ouvrir la page decesv.html dans un nouvel onglet
                const certificateUrl = 'decesv.html';
                console.log('📂 Ouverture de la page:', certificateUrl);
                
                // Essayer différentes méthodes pour ouvrir la page
                let opened = false;
                
                // Méthode 1: window.open avec URL relative
                try {
                    const newWindow = window.open(certificateUrl, '_blank');
                    if (newWindow && !newWindow.closed) {
                        console.log('✅ Page ouverte avec window.open');
                        opened = true;
                    }
                } catch (e) {
                    console.log('❌ window.open a échoué:', e.message);
                }
                
                // Méthode 2: URL absolue si la première méthode échoue
                if (!opened) {
                    try {
                        const absoluteUrl = new URL(certificateUrl, window.location.href).href;
                        window.open(absoluteUrl, '_blank');
                        console.log('✅ Page ouverte avec URL absolue');
                        opened = true;
                    } catch (e) {
                        console.log('❌ URL absolue a échoué:', e.message);
                    }
                }
                
                // Méthode 3: construction manuelle de l'URL
                if (!opened) {
                    const currentPath = window.location.pathname;
                    const directory = currentPath.substring(0, currentPath.lastIndexOf('/') + 1);
                    const fullUrl = window.location.origin + directory + certificateUrl;
                    window.open(fullUrl, '_blank');
                    console.log('✅ Page ouverte avec URL construite manuellement');
                }

                // NOTE: submitnew-top génère SEULEMENT le certificat HTML (pas d'enregistrement en base)
                // Utilisé pour réimprimer un certificat existant sans créer de doublon
                console.log('✅ Certificat HTML généré - Pas d\'enregistrement en base de données');

            } catch (error) {
                console.error('Erreur détaillée:', error);
                alert('Une erreur est survenue lors de la génération du certificat: ' + error.message);
            }
        });
    }

// Gestionnaire d'événements pour le bouton submit-update (Modification)
    if (document.getElementById('submit-update')) {
        document.getElementById('submit-update').addEventListener('click', async (e) => {
            try {
                console.log('💾 Enregistrement des modifications...');
                const formData = collectFormData();
                console.log('Données du formulaire collectées:', formData);

                // Sauvegarder les données dans localStorage pour la page decesv.html
                localStorage.setItem('certificateData', JSON.stringify(formData));
                console.log('✅ Données sauvegardées dans localStorage');

                // Ouvrir la page decesv.html dans un nouvel onglet
                const certificateUrl = 'decesv.html';
                console.log('📂 Ouverture de la page:', certificateUrl);
                
                // Essayer différentes méthodes pour ouvrir la page
                let opened = false;
                
                // Méthode 1: window.open avec URL relative
                try {
                    const newWindow = window.open(certificateUrl, '_blank');
                    if (newWindow && !newWindow.closed) {
                        console.log('✅ Page ouverte avec window.open');
                        opened = true;
                    }
                } catch (e) {
                    console.log('❌ window.open a échoué:', e.message);
                }
                
                // Méthode 2: URL absolue si la première méthode échoue
                if (!opened) {
                    try {
                        const absoluteUrl = new URL(certificateUrl, window.location.href).href;
                        window.open(absoluteUrl, '_blank');
                        console.log('✅ Page ouverte avec URL absolue');
                        opened = true;
                    } catch (e) {
                        console.log('❌ URL absolue a échoué:', e.message);
                    }
                }
                
                // Méthode 3: construction manuelle de l'URL
                if (!opened) {
                    const currentPath = window.location.pathname;
                    const directory = currentPath.substring(0, currentPath.lastIndexOf('/') + 1);
                    const fullUrl = window.location.origin + directory + certificateUrl;
                    window.open(fullUrl, '_blank');
                    console.log('✅ Page ouverte avec URL construite manuellement');
                }

                // Modifier le certificat dans la base de données
                console.log('💾 Modification du certificat dans la base de données...');
                await modifierCertificatDeces(formData);

                // Afficher un message de succès
                alert('✅ Certificat modifié avec succès dans la base de données !');

            } catch (error) {
                console.error('Erreur détaillée:', error);
                alert('Une erreur est survenue lors de la modification du certificat: ' + error.message);
            }
        });
    }
}



// Fonction pour générer et imprimer le certificat
async function generateCertificate(event) {
    try {
        event.preventDefault();

        // Collect form data using existing functions
        const formData = collectFormData();

        // Debug logging
        console.log('Wilaya Deces:', formData.wilayaDeces);
        console.log('Wilaya en arabe:', wilayaar[formData.wilayaDeces]);
        console.log('Wilaya avec 000:', wilayaar[formData.wilayaDeces + '000']);

// Debug: Vérifier la valeur de l'heure
        console.log('DEBUG - formData.heureDeces:', formData.heureDeces);
        console.log('DEBUG - typeof formData.heureDeces:', typeof formData.heureDeces);
        
        // Préparer les valeurs de conversion avant de générer le HTML
        let hourInArabic = '';
        let minuteInArabic = 'الصفر';
        
        if (formData.heureDeces) {
            console.log('DEBUG - Heure trouvée, traitement...');
            const timeParts = formData.heureDeces.split(':');
            console.log('DEBUG - timeParts:', timeParts);
            
            const hour = parseInt(timeParts[0], 10);
            const minutes = parseInt(timeParts[1], 10);
            
            console.log('DEBUG - hour:', hour, 'minutes:', minutes);
            
            // Conversion de l'heure
            if (!isNaN(hour) && hour >= 0 && hour <= 23) {
                let timeText;
                if (hour === 12) {
                    timeText = 'منتصف النهار';
                } else if (hour < 12) {
                    timeText = 'صباحا';
                } else {
                    timeText = 'زوالا';
                }
                
                const hoursArabic = [
                    "الواحدة", "الثانية", "الثالثة", "الرابعة", "الخامسة", "السادسة", "السابعة", "الثامنة", "التاسعة", "العاشرة",
                    "الحادية عشر", "الثانية عشر", "الثالثة عشر", "الرابعة عشر", "الخامسة عشر", "السادسة عشر", "السابعة عشر", "الثامنة عشر", "التاسعة عشر", "العشرون", "الواحدة و العشرون", "الثانية و العشرون", "الثالثة و العشرون", "الرابعة و العشرون"
                ];
                const h12 = hour > 12 ? hour - 12 : (hour === 0 ? 12 : hour);
                hourInArabic = (hoursArabic[h12 - 1] || '') + ' ' + timeText;
                console.log('DEBUG - hourInArabic calculé:', hourInArabic);
            }
            
            // Conversion des minutes
            if (!isNaN(minutes) && minutes >= 0 && minutes <= 59) {
                if (minutes === 0) {
                    minuteInArabic = 'الصفر';
                } else {
                    const minutesArabic = [
                        "الاولى", "الثانية", "الثالثة", "الرابعة", "الخامسة", "السادسة", "السابعة", "الثامنة", "التاسعة", "العاشرة",
                        "الحادي عشرة", "الثاني عشرة", "الثالث عشرة", "الرابع عشرة", "الخامس عشرة", "السادس عشرة", "السابع عشرة", "الثامن عشرة", "التاسع عشرة", "العشرين",
                        "الواحد و العشرين", "الثاني و العشرين", "الثالث و العشرين", "الرابع و العشرين", "الخامس و العشرين", "السادس و العشرين", "السابع و العشرين", "الثامن و العشرين", "التاسع و العشرين", "الثلاثين",
                        "الواحدة و الثلاثين", "الثانية و الثلاثين", "الثالثة و الثلاثين", "الرابعة و الثلاثين", "الخامسة و الثلاثين", "السادسة و الثلاثين", "السابعة و الثلاثين", "الثامنة و الثلاثين", "التاسعة و الثلاثين", "الأربعين",
                        "الواحدة و الأربعين", "الثانية و الأربعين", "الثالثة و الأربعين", "الرابعة و الأربعين", "الخامسة و الأربعين", "السادسة و الأربعين", "السابعة و الأربعين", "الثامنة و الأربعين", "التاسعة و الأربعين", "الخمسين",
                        "الواحدة و الخمسين", "الثانية و الخمسين", "الثالثة و الخمسين", "الرابعة و الخمسين", "الخامسة و الخمسين", "السادسة و الخمسين", "السابعة و الخمسين", "الثامنة و الخمسين", "التاسعة و الخمسين"
                    ];
                    minuteInArabic = minutesArabic[minutes - 1] || 'الصفر';
                }
                console.log('DEBUG - minuteInArabic calculé:', minuteInArabic);
            }
        } else {
            console.log('DEBUG - Aucune heure trouvée dans formData');
        }

        // Préparer les conversions de date
        const dateParts = formData.date_deces ? formData.date_deces.split('-') : ['', '', ''];
        const yearInArabic = (() => {
            const yearNum = parseInt(dateParts[0], 10);
            if (isNaN(yearNum) || yearNum < 2020 || yearNum > 2040) return dateParts[0];
            const years = {
                2020: 'الفين و عشرون',
                2021: 'الفين و واحد و عشرون',
                2022: 'الفين و اثنان و عشرون',
                2023: 'الفين و ثلاثة و عشرون',
                2024: 'الفين و اربعة و عشرون',
                2025: 'الفين و خمسة و عشرون',
                2026: 'الفين و ستة و عشرون',
                2027: 'الفين و سبعة و عشرون',
                2028: 'الفين و ثمانية و عشرون',
                2029: 'الفين و تسعة و عشرون',
                2030: 'الفين و ثلاثون',
                2031: 'الفين و واحد و ثلاثون',
                2032: 'الفين و اثنان و ثلاثون',
                2033: 'الفين و ثلاثة و ثلاثون',
                2034: 'الفين و اربعة و ثلاثون',
                2035: 'الفين و خمسة و ثلاثون',
                2036: 'الفين و ستة و ثلاثون',
                2037: 'الفين و سبعة و ثلاثون',
                2038: 'الفين و ثمانية و ثلاثون',
                2039: 'الفين و تسعة و ثلاثون',
                2040: 'الفين و اربعون'
            };
            return years[yearNum] || dateParts[0];
        })();

        const dayInArabic = (() => {
            const dayNum = parseInt(dateParts[2], 10);
            if (isNaN(dayNum) || dayNum < 1 || dayNum > 31) return dateParts[2];
            const days = [
                "الاول", "الثانى", "الثالث", "الرابع", "الخامس", "السادس", "السابع", "الثامن", "التاسع", "العاشر",
                "الحادي عشر", "الثاني عشر", "الثالث عشر", "الرابع عشر", "الخامس عشر", "السادس عشر", "السابع عشر",
                "الثامن عشر", "التاسع عشر", "العشرين", "الواحد و العشرين", "الثاني و العشرين", "الثالث و العشرين",
                "الرابع و العشرين", "الخامس و العشرين", "السادس و العشرين", "السابع عشر", "الثامن عشر",
                "التاسع عشر", "الثلاثين", "الواحد و الثلاثين"
            ];
            return days[dayNum - 1] || dateParts[2];
        })();

        const monthInArabic = (() => {
            const monthNum = parseInt(dateParts[1], 10);
            if (isNaN(monthNum) || monthNum < 1 || monthNum > 12) return dateParts[1];
            const months = [
                "", "جانفى", "فيفري", "مارس", "افريل", "ماي", "جوان", "جويلية", "اوت", "سبتمبر", "اكتوبر", "نوفمبر", "ديسمبر"
            ];
            return months[monthNum] || dateParts[1];
        })();

        console.log('Valeurs converties:', { hourInArabic, minuteInArabic, yearInArabic, dayInArabic, monthInArabic });

        // Échapper les valeurs pour éviter les erreurs de syntaxe
        const safeHourInArabic = hourInArabic.replace(/`/g, '\\`').replace(/\${/g, '\\${');
        const safeMinuteInArabic = minuteInArabic.replace(/`/g, '\\`').replace(/\${/g, '\\${');
        const safeYearInArabic = yearInArabic.replace(/`/g, '\\`').replace(/\${/g, '\\${');
        const safeDayInArabic = dayInArabic.replace(/`/g, '\\`').replace(/\${/g, '\\${');
        const safeMonthInArabic = monthInArabic.replace(/`/g, '\\`').replace(/\${/g, '\\${');

        // Create a new window for the certificate
        const win = window.open('', '_blank');
        
        // Construire le HTML en utilisant une méthode simple sans template strings complexes
        const buildHTML = function() {
            let html = '<!DOCTYPE html>';
            html += '<html lang="ar" dir="rtl">';
            html += '<head>';
            html += '<meta charset="UTF-8">';
            html += '<title>تصريح بالوفاة</title>';
html += '<style>';
            html += '@page { size: A4; margin: 0; }';
            html += '@media print {';
            html += 'body { margin: 0; padding: 0; background-color: #fff; -webkit-print-color-adjust: exact; print-color-adjust: exact; }';
            html += '.page { position: relative; width: 210mm; height: 297mm; margin: 0; padding: 10mm; box-sizing: border-box; overflow: hidden; }';
            html += '.rect-main { position: absolute; top: 5mm; left: 10mm; right: 10mm; width: 190mm; height: 277mm; border: 2px solid #000; border-radius: 2mm; }';
            html += '.rect-vertical { position: absolute; top: 50mm; left: 155mm; width: 2px; height: 220mm; border: 1px solid #000; }';
            html += '.header { position: absolute; top: 15mm; left: 15mm; right: 15mm; text-align: center; font-size: 12pt; padding: 3mm; }';
            html += '.title { position: absolute; top: 35mm; left: 15mm; right: 15mm; width: 170mm; text-align: center; font-size: 14pt; font-weight: bold; padding: 3mm; background-color: #f5f5f5; }';
            html += '.institution { position: absolute; top: 55mm; right: 15mm; width: 35mm; height: 15mm; text-align: right; font-size: 10pt; padding: 2mm; background-color: #f5f5f5; }';
            html += '.info-box { position: absolute; top: 80mm; right: 15mm; width: 45mm; padding: 3mm; text-align: right; font-size: 10pt; }';
            html += '.info-box p { margin: 2mm 0; }';
            html += '.content { position: absolute; top: 55mm; left: 165mm; right: 85mm; width: 70mm; font-size: 10pt; line-height: 1.3; text-align: right; }';
            html += '.signature { position: absolute; top: 200mm; left: 15mm; right: 15mm; width: 180mm; height: 70mm; }';
            html += '.signature-area { position: absolute; top: 5mm; left: 5mm; width: 75mm; padding: 2mm; text-align: center; font-size: 10pt; }';
            html += '.name-area { position: absolute; top: 5mm; right: 1mm; width: 75mm; padding: 2mm; text-align: right; font-size: 10pt; }';
            html += '.print-button { display: none !important; }';
            html += '}';
            html += '@media screen {';
            html += 'body { font-family: Arial, sans-serif; margin: 20px; padding: 20px; background-color: #f0f0f0; }';
            html += '.page { position: relative; width: 210mm; height: 287mm; margin: 20px auto; padding: 10mm; background-color: #fff; box-shadow: 0 0 10px rgba(0,0,0,0.3); }';
            html += '.rect-main { position: absolute; top: 5mm; left: 0; right: 0; width: 220mm; height: 287mm; border: 2px solid #000; border-radius: 2mm; }';
            html += '.rect-vertical { position: absolute; top: 46mm; left: 155mm; width: 2px; height: 244mm; border: 1px solid #000; }';
            html += '.header { position: absolute; top: 10mm; left: 0; right: 0; text-align: center; font-size: 14pt; padding: 5mm; }';
            html += '.title { position: absolute; top: 40mm; left: 8; right:1; width: 200mm; text-align: center; font-size: 16pt; font-weight: bold; padding: 5mm; background-color: #f5f5f5; }';
            html += '.institution { position: absolute; top: 70mm; right: 5mm; width: 60mm; height: 20mm; text-align: right; font-size: 12pt; padding: 4mm; background-color: #f5f5f5; }';
            html += '.info-box { position: absolute; top: 100mm; right: 10mm; width: 50mm; padding: 5mm; text-align: right; font-size: 12pt; }';
            html += '.info-box p { margin: 5mm 0; }';
            html += '.content { position: absolute; top: 80mm; right: 80mm; left: 20mm; font-size: 13pt; line-height: 1.5; text-align: right; }';
            html += '.signature { position: absolute; top: 200mm; left: 15mm; right: 15mm; width: 180mm; height: 70mm; }';
            html += '.signature-area { position: absolute; top: 5mm; left: 5mm; width: 75mm; padding: 2mm; text-align: center; }';
            html += '.name-area { position: absolute; top: 5mm; right: 2mm; width: 75mm; padding: 2mm; text-align: right; }';
            html += '.print-button { text-align: center; margin-top: 20px; position: fixed; top: 20px; right: 20px; z-index: 1000; }';
            html += '.print-button button { padding: 10px 20px; font-size: 16px; background-color: #007bff; color: white; border: none; border-radius: 5px; cursor: pointer; }';
            html += '.print-button button:hover { background-color: #0056b3; }';
            html += '}';
            html += '</style>';
            html += '</head>';
            html += '<body>';
            html += '<div class="page">';
            html += '<div class="rect-main"></div>';
            html += '<div class="rect-vertical"></div>';
            html += '<div class="header">';
            html += 'الجمـهوريـــة الجزائرية الديمقراطية الشعبيــــــــة <br>';
            html += 'وزارة الصحــــــــة و السكـــــــــان وإصلاح المستشــــــفيات <br>';
            html += 'مـديريــــــة الصحــــة و الــــسكان لولايـــــة ' + (formData.wilayaDeces ? (wilayaar[formData.wilayaDeces + '000'] || wilayaar[formData.wilayaDeces] || formData.wilayaDeces) : '');
            html += '</div>';
            html += '<div class="title">تصريح بالوفاة</div>';
            html += '<div class="info-box">';
            html += '<p>الرقم : ........</p>';
            html += '<p>دفتر عائلي رقم : ..........</p>';
            html += '<p>صادر بتاريخ : ..............</p>';
            html += '<p>ولاية : ...................</p>';
            html += '</div>';
            html += '<div class="institution">' + formData.institution + '<br>' + formData.clinique + '</div>';
            html += '<div class="content">';
            html += 'في عام ' + safeYearInArabic + '<br>';
            html += 'و في اليوم ' + safeDayInArabic + '<br>';
            html += 'من شهر ' + safeMonthInArabic + '<br>';
            html += 'نحن مدير ' + formData.institution + '<br>';
            html += 'نشعر رئيس المجلس الشعبي البلدي ضابط الحالة المدنية <br>';
            html += 'انه و في هذا اليوم و على الساعة <span class="select-container"><span class="print-value">' + safeHourInArabic + '</span></span><br>';
            html += 'والدقيقة <span class="select-container"><span class="print-value">' + safeMinuteInArabic + '</span></span><br>';
            html += 'توفي (ت) المسمى (ة) : ' + formData.nom_ar + '&nbsp;&nbsp;&nbsp;' + formData.prenom_ar + '<br>';
            html += 'المولود (ة) في ' + formatDate(formData.dateNaissance) + '<br>';
            html += 'بـ : ' + (formData.wilaya_naissance ? (wilayaar[formData.wilaya_naissance + '000'] || wilayaar[formData.wilaya_naissance] || formData.wilaya_naissance) : '') + '<br>';
            html += 'إبن (ة) : ' + formData.perear + '<br>';
            html += 'و : ' + formData.merear + '<br>';
            html += 'زوج (ة) : ' + formData.conjoint + '<br>';
            html += 'المهنة : ' + formData.profession + '<br>';
            html += 'العنوان الحالي : ' + formData.adresse + '<br>';
            html += 'دخل (ت) الى المستشفى يوم : ' + formatDate(formData.date_entree) + '<br>';
            html += 'و توفي (ت) يوم : ' + formatDate(formData.date_deces) + '<br>';
            html += 'على الساعة : ' + formData.heureDeces + '<br>';
            html += '</div>';
            html += '<div class="signature">';
            html += '<div class="signature-area">';
            html += 'في : ' + formData.place + '&nbsp;&nbsp;&nbsp;&nbsp;بتاريخ :..' + formatDate(formData.DSG) + '<br>';
            html += 'إمضاء المدير<br>';
            html += '.................<br><br>';
            html += '<br>';
            html += '</div>';
            html += '<div class="name-area">';
            html += 'إمضاء الطبيـب<br>';
            html += formData.medecin + '<br><br><br><br>';
            html += 'الكتابة السابقة للإسم و اللقب<br>';
            html += formData.nom + '<br><br>';
            html += formData.prenom;
            html += '</div>';
            html += '<div class="print-button">';
            html += '<button class="print-button" onclick="window.print()">Imprimer</button>';
            html += '</div>';
            html += '</div>';
            html += '</div>';
            html += '</body>';
            html += '</html>';
            return html;
        };
        
        win.document.write(buildHTML());
        win.document.close();
    } catch (error) {
        console.error('Erreur lors de la génération du certificat:', error);
        alert('Une erreur est survenue lors de la génération du certificat.');
    }
}


// Fonction de changement d'onglet améliorée
function tabSwitch(tabId, contentId) {
    // Désactiver tous les onglets
    document.querySelectorAll('.tabs a').forEach(tab => {
        tab.classList.remove('active');
    });

    // Masquer tous les contenus
    document.querySelectorAll('.contenttabs').forEach(content => {
        content.style.display = 'none';
    });

    // Activer l'onglet et le contenu sélectionné
    const activeTab = document.getElementById(tabId);
    const activeContent = document.getElementById(contentId);

    if (activeTab && activeContent) {
        activeTab.classList.add('active');
        activeContent.style.display = 'block';
    } else {
        console.error('Onglet ou contenu introuvable :', { tabId, contentId });
    }
}

// Initialisation
// Fonction pour mettre à jour les valeurs affichées lors de l'impression
document.addEventListener('DOMContentLoaded', function () {
    // Pré-remplir les champs depuis le localStorage avec capitalisation
    const nom = capitalizeNames(localStorage.getItem('nom') || '');
    const prenom = capitalizeNames(localStorage.getItem('prenom') || '');
    const datens = localStorage.getItem('dateNaissance') || '';
    const polyAr = localStorage.getItem('polyclinique-ar') || '';

    const nomInput = document.getElementById('NOM');
    const prenomInput = document.getElementById('PRENOM');
    const datensInput = document.getElementById('DATENS');
    if (nomInput) nomInput.value = nom;

    // Initialiser les valeurs affichées pour les selects
    const selectContainers = document.querySelectorAll('.select-container');
    selectContainers.forEach(container => {
        const select = container.querySelector('select');
        const printValue = container.querySelector('.print-value');
        if (select && printValue) {
            printValue.textContent = select.options[select.selectedIndex].text;
            select.addEventListener('change', function () {
                printValue.textContent = this.options[this.selectedIndex].text;
            });
        }
    });
});

// Fonction pour capitaliser automatiquement les noms et prénoms
function capitalizeNames(text) {
    if (!text) return text;
    return text.toLowerCase().replace(/\b\w/g, letter => letter.toUpperCase());
}

// Pré-remplir les champs depuis le localStorage
document.addEventListener('DOMContentLoaded', function () {
    // Pré-remplir les champs depuis le localStorage avec capitalisation
    const nom = capitalizeNames(localStorage.getItem('nom') || '');
    const prenom = capitalizeNames(localStorage.getItem('prenom') || '');
    const datens = localStorage.getItem('dateNaissance') || '';
    const polyAr = localStorage.getItem('polyclinique-ar') || '';

    const nomInput = document.getElementById('NOM');
    const prenomInput = document.getElementById('PRENOM');
    const datensInput = document.getElementById('DATENS');
    if (nomInput) nomInput.value = nom;
    if (prenomInput) prenomInput.value = prenom;
    if (datensInput) datensInput.value = datens;
    // Remplir le champ CLINIQUE automatiquement
    const cliniqueInput = document.getElementById('CLINIQUE');
    if (cliniqueInput) cliniqueInput.value = polyAr;


    // Initialiser les onglets
    setupTabs();

    // Initialiser les sélecteurs CIM-10
    if (typeof initializeCIM10Selectors === 'function') {
        initializeCIM10Selectors();
    }

    // Initialiser les dates avec la date du jour
    const today = new Date();
    const dateString = today.toISOString().split('T')[0];
    document.getElementById('DSG').value = dateString;
    document.getElementById('DINS').value = dateString;

    // Configurer les gestionnaires d'événements
    setupArabicInputHandlers();
    setupFormListeners();

    // Configurer les gestionnaires de formulaire
    const form = document.querySelector('form');
    if (form) {
        form.addEventListener('submit', handleFormSubmit);
    }

    // Configurer les gestionnaires de code P
    const WILAYAD = document.getElementById('WILAYAD');
    const NDLMAAP = document.getElementById('NDLMAAP');
    if (WILAYAD) {
        WILAYAD.addEventListener('blur', genererCodeP);
    }
    if (NDLMAAP) {
        NDLMAAP.addEventListener('blur', genererCodeP);
    }

    // Initialiser les codes
    remplir1();
    remplir2();
    remplir3();
});

// Fonctions d'initialisation
function setupFormListeners() {
    const form = document.querySelector('form');
    if (form) {
        form.addEventListener('submit', handleFormSubmit);
        document.getElementById('WILAYAD').addEventListener('blur', genererCodeP);
        document.getElementById('NDLMAAP').addEventListener('blur', myFunction1);

        configureUppercaseInputs();
        configureBlurHandlers();
    }
}

function initializeTabs() {
    document.querySelectorAll('.tabs a').forEach(tab => {
        tab.addEventListener('click', function (e) {
            e.preventDefault();
            const target = this.getAttribute('href').split('_')[1];
            tabSwitch(`tab_${target}`, `content_${target}`);
        });
    });
}

// Fonction pour formater la date au format jj-mm-aaaa
function formatDate(date) {
    if (!date) return '';
    // Si c'est déjà une date, on la formate
    if (date instanceof Date) {
        return date.toLocaleDateString('fr-FR', {
            day: '2-digit',
            month: '2-digit',
            year: 'numeric'
        });
    }
    // Sinon, on essaye de convertir la chaîne en date
    const d = new Date(date);
    if (isNaN(d.getTime())) return '';
    return d.toLocaleDateString('fr-FR', {
        day: '2-digit',
        month: '2-digit',
        year: 'numeric'
    });
}
// Fonction pour récupérer la valeur d'un élément de formulaire
function getElementValue(elementId) {
    const element = document.getElementById(elementId);
    if (!element) return '';

    if (element.type === 'select-one') {
        return element.value;
    } else if (element.type === 'radio') {
        const radioButtons = document.getElementsByName(element.name);
        for (let i = 0; i < radioButtons.length; i++) {
            if (radioButtons[i].checked) {
                return radioButtons[i].value;
            }
        }
        return '';
    } else {
        return element.value || '';
    }
}

// Fonction pour récupérer la valeur d'un groupe de radio buttons
function getRadioValue(name, defaultValue = '') {
    const radioButtons = document.getElementsByName(name);
    for (let i = 0; i < radioButtons.length; i++) {
        if (radioButtons[i].checked) {
            return radioButtons[i].value;
        }
    }
    return defaultValue;
}

// Fonction pour calculer l'âge
function calculateAge(birthDate, deathDate) {
    if (!birthDate || !deathDate) return '';

    // Convertir les dates en objets Date en gérant le format YYYY-MM-DD
    const birth = new Date(birthDate);
    const death = new Date(deathDate);

    // Vérifier si les dates sont valides
    if (isNaN(birth.getTime()) || isNaN(death.getTime())) {
        console.error('Date invalide :', { birthDate, deathDate });
        return '';
    }

    // Vérifier si la date de décès est antérieure à la date de naissance
    if (death < birth) {
        return {
            years: '00',
            months: '',
            days: '0 jour(s)'
        };
    }

    // Calculer les différences initiales
    let years = death.getFullYear() - birth.getFullYear();
    let months = death.getMonth() - birth.getMonth();
    let days = death.getDate() - birth.getDate();

    // Ajuster les jours
    if (days < 0) {
        months--;
        const lastDayOfPrevMonth = new Date(death.getFullYear(), death.getMonth(), 0).getDate();
        days += lastDayOfPrevMonth;
    }

    // Ajuster les mois
    if (months < 0) {
        months += 12;
        years--;
    }

    // Afficher les valeurs calculées
    console.log(`Années : ${years}, Mois : ${months}, Jours : ${days}`);

    // Logique pour afficher uniquement les années si >= 1 an
    if (years >= 1) {
        return {
            years: years.toString(),
            months: '',
            days: ''
        };
    } else if (months > 0) {
        return {
            years: '00',
            months: months.toString(),
            days: ''
        };
    } else {
        return {
            years: '00',
            months: '',
            days: days.toString()
        };
    }
}


// Fonction pour convertir le sexe en Masculin ou Féminin
function getSexeCode(sexe) {
    return sexe === 'M' ? 'Masculin' : sexe === 'F' ? 'Féminin' : '';
}

// Fonction pour récupérer la valeur d'une case à cocher
function getCheckboxValue(name) {
    const checkbox = document.querySelector(`input[name="${name}"]:checked`);
    return checkbox ? checkbox.value : '';
}

// Fonction pour convertir le numéro de wilaya en nom


// Fonction pour convertir le numéro de wilaya en nom (arabe ou français)
function getWilayaName(wilayaNumber, lang = 'ar') {
    console.log('getWilayaName input:', wilayaNumber, 'lang:', lang);

    // Vérifier si le numéro est valide
    if (!wilayaNumber || isNaN(wilayaNumber)) {
        console.log('Invalid wilaya number');
        return '';
    }

    // Convertir le numéro de wilaya en numéro standard
    let wilayaCode = parseInt(wilayaNumber);
    console.log('Parsed wilayaCode:', wilayaCode);

    // Si c'est déjà un code complet (avec 000)
    if (wilayaCode % 1000 === 0) {
        wilayaCode = String(wilayaCode).padStart(5, '0');
    } else {
        // Sinon, c'est juste le numéro de wilaya
        wilayaCode = String(wilayaCode * 1000).padStart(5, '0');
    }
    console.log('Formatted wilayaCode:', wilayaCode);

    // Récupérer le nom selon la langue demandée
    console.log('Looking up wilaya with code:', wilayaCode);
    console.log('Available wilaya codes:', Object.keys(wilayaar));

    if (lang.toLowerCase() === 'fr') {
        const name = wilayas[wilayaCode] || '';
        console.log('French name:', name);
        return name;
    } else {
        const name = wilayaar[wilayaCode] || '';
        console.log('Arabic name:', name);
        return name;
    }
}
// Convertir les chiffres en chiffres arabes
function convertToArabicNumbers(num) {
    const arabicNumbers = ['٠', '١', '٢', '٣', '٤', '٥', '٦', '٧', '٨', '٩'];
    return num.toString().split('').map(digit => arabicNumbers[digit]).join('');
}


// Convertir l'heure en arabe
function convertHourToArabic(time) {
    if (!time) {
        console.log('No time provided to convertHourToArabic');
        return '';
    }

    // Debug logging
    console.log('Input time:', time);

    // Get hours
    const hours = parseInt(time.split(':')[0], 10);
    console.log('Parsed hours:', hours);

    if (isNaN(hours) || hours < 0 || hours > 23) {
        console.log('Invalid hours');
        return '';
    }

    const hoursArabic = [
        "الواحدة", "الثانية", "الثالثة", "الرابعة", "الخامسة", "السادسة", "السابعة", "الثامنة", "التاسعة", "العاشرة",
        "الحادية عشر", "الثانية عشر", "الثالثة عشر", "الرابعة عشر", "الخامسة عشر", "السادسة عشر", "السابعة عشر", "الثامنة عشر", "التاسعة عشر", "العشرون", "الواحدة و العشرون", "الثانية و العشرون", "الثالثة و العشرون", "الرابعة و العشرون"
    ];

    // Convert 24h to 12h format
    const h12 = hours > 12 ? hours - 12 : (hours === 0 ? 12 : hours);
    return hoursArabic[h12 - 1] || '';
}

// Convertir les minutes en arabe
function convertMinuteToArabic(time) {
    if (!time) {
        console.log('No time provided to convertMinuteToArabic');
        return 'الصفر';
    } // retourne 'الصفر' si pas de temps

    // Debug logging
    console.log('Input time for minutes:', time);

    // Get minutes
    const minutes = parseInt(time.split(':')[1], 10);
    console.log('Parsed minutes:', minutes);

    if (isNaN(minutes) || minutes < 0 || minutes > 59) {
        console.log('Invalid minutes');
        return 'الصفر';
    }

    if (minutes === 0) return 'الصفر';

    const minutesArabic = [
        "الاولى", "الثانية", "الثالثة", "الرابعة", "الخامسة", "السادسة", "السابعة", "الثامنة", "التاسعة", "العاشرة",
        "الحادي عشرة", "الثاني عشرة", "الثالث عشرة", "الرابع عشرة", "الخامس عشرة", "السادس عشرة", "السابع عشرة", "الثامن عشرة", "التاسع عشرة", "العشرين",
        "الواحد و العشرين", "الثاني و العشرين", "الثالث و العشرين", "الرابع و العشرين", "الخامس و العشرين", "السادس و العشرين", "السابع و العشرين", "الثامن و العشرين", "التاسع و العشرين", "الثلاثين",
        "الواحدة و الثلاثين", "الثانية و الثلاثين", "الثالثة و الثلاثين", "الرابعة و الثلاثين", "الخامسة و الثلاثين", "السادسة و الثلاثين", "السابعة و الثلاثين", "الثامنة و الثلاثين", "التاسعة و الثلاثين", "الأربعين",
        "الواحدة و الأربعين", "الثانية و الأربعين", "الثالثة و الأربعين", "الرابعة و الأربعين", "الخامسة و الأربعين", "السادسة و الأربعين", "السابعة و الأربعين", "الثامنة و الأربعين", "التاسعة و الأربعين", "الخمسين",
        "الواحدة و الخمسين", "الثانية و الخمسين", "الثالثة و الخمسين", "الرابعة و الخمسين", "الخامسة و الخمسين", "السادسة و الخمسين", "السابعة و الخمسين", "الثامنة و الخمسين", "التاسعة و الخمسين"
    ];

    return minutesArabic[minutes - 1] || 'الصفر';
}

// Convertir le mois en arabe
function convertMonthToArabic(month) {
    // Debug logging
    console.log('Input month:', month);

    // Convert string to number if it's a string
    const monthNum = parseInt(month, 10);
    console.log('Parsed month:', monthNum);

    // Return empty if not a valid month
    if (isNaN(monthNum) || monthNum < 1 || monthNum > 12) {
        console.log('Invalid month');
        return '';
    }

    const months = [
        "",
        "جانفى",
        "فيفري",
        "مارس",
        "افريل",
        "ماي",
        "جوان",
        "جويلية",
        "اوت",
        "سبتمبر",
        "اكتوبر",
        "نوفمبر",
        "ديسمبر"
    ];

    return months[monthNum] || '';
}

// Convertir le jour en mots arabes
function convertDayToArabicWords(day) {
    // Debug logging
    console.log('Input day:', day);

    // Convert string to number if it's a string
    const dayNum = parseInt(day, 10);
    console.log('Parsed day:', dayNum);

    // Return empty if not a valid day
    if (isNaN(dayNum) || dayNum < 1 || dayNum > 31) {
        console.log('Invalid day');
        return '';
    }

    const days = [
        "الاول", "الثانى", "الثالث", "الرابع", "الخامس", "السادس", "السابع", "الثامن", "التاسع", "العاشر",
        "الحادي عشر", "الثاني عشر", "الثالث عشر", "الرابع عشر", "الخامس عشر", "السادس عشر", "السابع عشر",
        "الثامن عشر", "التاسع عشر", "العشرين", "الواحد و العشرين", "الثاني و العشرين", "الثالث و العشرين",
        "الرابع و العشرين", "الخامس و العشرين", "السادس و العشرين", "السابع و العشرين", "الثامن و العشرين",
        "التاسع و العشرين", "الثلاثين", "الواحد و الثلاثين"
    ];

    return days[dayNum - 1] || ''; // -1 car le tableau commence à 0
}

// Convertir l'année en mots arabes
function convertYearToArabicWords(year) {
    // Debug logging
    console.log('Input year:', year);

    // Convert string to number if it's a string
    const yearNum = parseInt(year, 10);
    console.log('Parsed year:', yearNum);

    // Return empty if not a valid year
    if (isNaN(yearNum) || yearNum < 2020 || yearNum > 2040) {
        console.log('Invalid year');
        return '';
    }

    const years = {
        2020: 'الفين و عشرون',
        2021: 'الفين و واحد و عشرون',
        2022: 'الفين و اثنان و عشرون',
        2023: 'الفين و ثلاثة و عشرون',
        2024: 'الفين و اربعة و عشرون',
        2025: 'الفين و خمسة و عشرون',
        2026: 'الفين و ستة و عشرون',
        2027: 'الفين و سبعة و عشرون',
        2028: 'الفين و ثمانية و عشرون',
        2029: 'الفين و تسعة و عشرون',
        2030: 'الفين و ثلاثون',
        2031: 'الفين و واحد و ثلاثون',
        2032: 'الفين و اثنان و ثلاثون',
        2033: 'الفين و ثلاثة و ثلاثون',
        2034: 'الفين و اربعة و ثلاثون',
        2035: 'الفين و خمسة و ثلاثون',
        2036: 'الفين و ستة و ثلاثون',
        2037: 'الفين و سبعة و ثلاثون',
        2038: 'الفين و ثمانية و ثلاثون',
        2039: 'الفين و تسعة و ثلاثون',
        2040: 'الفين و اربعون'
    };
    return years[year] || ''; // Valeur par défaut
}




// Fonction pour initialiser le document PDF pour la soumission
async function initializePDFDocumentForSubmit() {
    try {
        // Initialiser le document PDF
        const pdfDoc = await PDFLib.PDFDocument.create();

        // Ajouter une page
        const page = pdfDoc.addPage([595.28, 841.89]); // Taille A4

        // Ajouter la police
        const font = await pdfDoc.embedFont(PDFLib.StandardFonts.Helvetica);

        return { pdfDoc, page, font };
    } catch (error) {
        console.error('Erreur lors de l\'initialisation du document PDF:', error);
        throw error;
    }
}

// Liste des wilayas en français
const wilayas = {
    '17000': 'Djelfa',
    '18000': 'Jijel',
    '19000': 'Sétif',
    '20000': 'Saïda',
    '21000': 'Skikda',
    '22000': 'Sidi Bel Abbès',
    '23000': 'Annaba',
    '24000': 'Guelma',
    '25000': 'Constantine',
    '26000': 'Médéa',
    '27000': 'Mostaganem',
    '28000': 'M\'Sila',
    '29000': 'Mascara',
    '30000': 'Ouargla',
    '31000': 'Oran',
    '32000': 'El Bayadh',
    '33000': 'Illizi',
    '34000': 'Bordj Bou Arréridj',
    '35000': 'Boumerdès',
    '36000': 'El Tarf',
    '37000': 'Tindouf',
    '38000': 'Tissemsilt',
    '39000': 'El Oued',
    '40000': 'Khenchela',
    '41000': 'Souk Ahras',
    '42000': 'Tipaza',
    '43000': 'Mila',
    '44000': 'Aïn Defla',
    '45000': 'Naâma',
    '46000': 'Aïn Témouchent',
    '47000': 'Ghardaïa',
    '48000': 'Relizane',
    '49000': 'Timimoun',
    '50000': 'Bordj Badji Mokhtar',
    '51000': 'Ouled Djellal',
    '52000': 'Béni Abbès',
    '53000': 'In Salah',
    '54000': 'In Guezzam',
    '55000': 'Touggourt',
    '56000': 'Djanet',
    '57000': 'El M\'Ghair',
    '58000': 'El Meniaa',
    '99000': 'Etranger'
};

// Liste des wilayas en arabe
const wilayaar = {
    '1000': 'أدرار',
    '2000': 'الشلف',
    '3000': 'الأغواط',
    '4000': 'أم البواقي',
    '5000': 'باتنة',
    '6000': 'بجاية',
    '7000': 'بسكرة',
    '8000': 'بشار',
    '9000': 'البليدة',
    '10000': 'البويرة',
    '11000': 'تمنراست',
    '12000': 'تبسة',
    '13000': 'تلمسان',
    '14000': 'تيارت',
    '15000': 'تيزي وزو',
    '16000': 'الجزائر',
    '17000': 'الجلفة',
    '18000': 'جيجل',
    '19000': 'سطيف',
    '20000': 'سعيدة',
    '21000': 'سكيكدة',
    '22000': 'سيدي بلعباس',
    '23000': 'عنابة',
    '24000': 'قالمة',
    '25000': 'قسنطينة',
    '26000': 'المدية',
    '27000': 'مستغانم',
    '28000': 'المسيلة',
    '29000': 'معسكر',
    '30000': 'ورقلة',
    '31000': 'ورقلة',
    '32000': 'البيض',
    '33000': 'إليزي',
    '34000': 'برج بوعريريج',
    '35000': 'بومرداس',
    '36000': 'الطارف',
    '37000': 'تيندوف',
    '38000': 'تيسمسيلت',
    '39000': 'وادي سوف',
    '40000': 'سوق أهراس',
    '42000': 'تيبازة',
    '43000': 'ميلة',
    '44000': 'عين الدفلى',
    '45000': 'النعامة',
    '46000': 'غرداية',
    '47000': 'غليزان',
    '49000': 'تيميمون',
    '50000': 'برج باجي مختار',
    '51000': 'أولاد جلال',
    '52000': 'بني عباس',
    '53000': 'عين صالح',
    '54000': 'عين قزام',
    '55000': 'توقرت',
    '56000': 'جانت',
    '57000': 'المغير',
    '58000': 'المنيعة',
    '99000': 'خارج الوطن'
};

async function generatePDFForSubmit(e) {
    if (e) e.preventDefault();

    try {
        const formData = collectFormData();

        // Charger le PDF existant
        const pdfBytes = await fetch('decesv.pdf').then(res => res.arrayBuffer());
        const pdfDoc = await PDFLib.PDFDocument.load(pdfBytes);

        // Récupérer la première page
        const page = pdfDoc.getPage(0);

        // Ajouter la police
        const font = await pdfDoc.embedFont(PDFLib.StandardFonts.Helvetica);

        // Dessiner les informations
        drawDeathInfo(page, font, formData);

        // Sauvegarder et ouvrir le PDF
        const pdfOutput = await pdfDoc.save();
        const blob = new Blob([pdfOutput], { type: 'application/pdf' });
        const url = URL.createObjectURL(blob);

        // Créer un lien et le cliquer pour télécharger
        const link = document.createElement('a');
        link.href = url;
        link.download = 'declaration_deces.pdf';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);

        alert('Certificat généré avec succès !');
        document.getElementById('decesForm').reset();
    } catch (error) {
        console.error('Erreur lors de la génération du PDF:', error);
        alert('Erreur lors de la génération du PDF. Veuillez vérifier les données saisies et réessayer.');
    }
}



// Setup event handlers for data attributes
/*function setupEventHandlers() {
    // Add event listeners for data-onblur attributes
    document.querySelectorAll('[data-onblur]').forEach(element => {
        const handlerName = element.getAttribute('data-onblur');
        if (window[handlerName]) {
            element.addEventListener('blur', window[handlerName]);
        }
    });
 
    // Add event listener for submit button
    const submitButton = document.getElementById('submitnew');
    if (submitButton) {
        const handlerName = submitButton.getAttribute('data-onclick');
        if (window[handlerName]) {
            submitButton.addEventListener('click', function(e) {
                e.preventDefault();
                window[handlerName]();
            });
        }
    }
}
*/
// Gestion de l'arabe
function setupArabicInputHandlers() {
    const arabicMap = {
        65: 'ض', 90: 'ص', 69: 'ث', 82: 'ق', 84: 'ف',
        89: 'غ', 85: 'ع', 73: 'ه', 79: 'خ', 80: 'ح',
        81: 'ش', 83: 'س', 68: 'ي', 70: 'ب', 71: 'ل',
        72: 'ا', 74: 'ت', 75: 'ن', 76: 'م', 77: 'ك',
        188: 'ة', 86: 'ر'
    };

    document.querySelectorAll('#place,#NOMAR, #PRENOMAR, #FILSDEAR, #ETDEAR, #NOMPRENOMAR, #ADAR')
        .forEach(input => {
            input.addEventListener('keydown', e => {
                if (arabicMap[e.keyCode]) {
                    e.preventDefault();
                    e.target.value += arabicMap[e.keyCode];
                }
            });
        });
}

// Configuration du formulaire
function configureUppercaseInputs() {
    document.querySelectorAll('[data-uppercase]').forEach(input => {
        input.addEventListener('input', e => {
            e.target.value = e.target.value.toUpperCase();
        });
    });
}

function configureBlurHandlers() {
    document.querySelectorAll('[data-onblur]').forEach(input => {
        input.addEventListener('blur', e => {
            const handler = window[e.target.dataset.onblur];
            if (typeof handler === 'function') handler(e);
        });
    });
}

// Fonction pour ajuster la hauteur de la textarea
function adjustTextareaHeight() {
    const textarea = document.querySelector('textarea[placeholder=" "]');
    if (textarea) {
        textarea.style.height = 'auto';
        textarea.style.height = (textarea.scrollHeight + 10) + 'px';
        textarea.style.minHeight = '100px';
    }
}

// Initialiser l'ajustement de la hauteur de la textarea
adjustTextareaHeight();

// Écouter les changements dans la textarea
const textarea = document.querySelector('textarea[placeholder=" "]');
if (textarea) {
    textarea.addEventListener('input', adjustTextareaHeight);
}

// Fonctions à implémenter (exemples)
// Fonctions à implémenter (exemples)
function remplir1() {
    // Générer les codes P
    const WILAYAD = document.getElementById('WILAYAD');
    const NDLMAAP = document.getElementById('NDLMAAP');
    const CODEP = document.getElementById('CODEP');

    if (WILAYAD && NDLMAAP && CODEP) {
        const codeP = `${WILAYAD.value}${NDLMAAP.value}`;
        CODEP.value = codeP;
    }
}

function remplir2() {
    // Générer le code C
    const CODEP = document.getElementById('CODEP');
    const CODEC = document.getElementById('CODEC');

    if (CODEP && CODEC && CODEP.value) {
        const codeC = CODEP.value.substring(0, 6);
        CODEC.value = codeC;
    }
}

function remplir3() {
    // Générer le code N
    const CODEC = document.getElementById('CODEC');
    const CODEN = document.getElementById('CODEN'); // Supposons que l'ID cible est CODEN, à vérifier si c'est CODEC ou autre

    if (CODEC && CODEC.value) {
        const codeN = CODEC.value.substring(0, 3);
        // document.getElementById('CODEN').value = codeN; // Commenté si l'élément n'existe pas
    }
}


function collectFormData() {
    const data = {
        nom: getElementValue('NOM'),
        prenom: getElementValue('PRENOM'),
        dateNaissance: getElementValue('DATENS'),
        sexe: getElementValue('SEXE'),
        pere: getElementValue('FILSDE'),
        mere: getElementValue('ETDE'),
        communeNaissance: getElementValue('COMMUNEN'),
        wilaya_naissance: getElementValue('WILAYAN'),
        wilayaResidence: getElementValue('WILAYAR'),
        place: getElementValue('place'),
        placefr: getElementValue('placefr'),
        DSG: getElementValue('DSG'),
        dateDeces: getElementValue('DINS'),
        heureDeces: getElementValue('HINS'),
        communeDeces: getElementValue('COMMUNED'),
        wilayaDeces: getElementValue('WILAYAD'),
        medecin: getElementValue('MEDECINHOSPIT'),
        adresse: getElementValue('ADAR'),
        // Ajout des champs pour la correspondance avec decesv.html
        datePresume: document.getElementById('DATEPRESUME') ? document.getElementById('DATEPRESUME').checked : false,
        DECEMAT: getCheckboxValue('DECEMAT'),
        DGRO: getRadioValue('GRS', 'DGRO'),
        DACC: getRadioValue('GRS', 'DACC'),
        DAVO: getRadioValue('GRS', 'DAVO'),
        AGESTATION: getRadioValue('GRS', 'AGESTATION'),
        IDETER: getRadioValue('GRS', 'IDETER'),
        GM: getCheckboxValue('GM'),
        MN: getCheckboxValue('MN'),
        AGEGEST: getElementValue('AGEGEST', '00'),
        POIDNSC: getElementValue('POIDNSC', '0000'),
        AGEMERE: getElementValue('AGEMERE', '00'),
        DPNAT: getCheckboxValue('DPNAT'),
        EMDPNAT: getElementValue('EMDPNAT', '').toUpperCase(),
        communeResidence: getElementValue('COMMUNER'),
        lieuDeces: getRadioValue('LD'),
        autresLieuDeces: getElementValue('LD6'),
        causeDeces: document.querySelector('input[name="CD"]:checked')?.value || '',
        causeDirecte: getElementValue('CAUSEDIRECTE'),
        etatMorbide: getRadioValue('ETATMORBIDE'),
        natureMort: getRadioValue('NDLM'),
        natureMortAutre: getElementValue('NDLMAAP'),
        obstacleMedicoLegal: getCheckboxValue('LD8'),
        contamination: getCheckboxValue('LD9'),
        prothese: getCheckboxValue('LD10'),
        POSTOPP2: getCheckboxValue('POSTOPP2'),
        CIM1: getElementValue('CIM1'),
        CIM2: getElementValue('CIM2'),
        CIM3: getElementValue('CIM3'),
        CIM4: getElementValue('CIM4'),
        CIM5: getElementValue('CIM5'),
        nom_ar: getElementValue('NOMAR'),
        prenom_ar: getElementValue('PRENOMAR'),
        perear: getElementValue('FILSDEAR'),
        merear: getElementValue('ETDEAR'),
        lieu_naissance: getElementValue('LIEUNAIS'),
        conjoint: getElementValue('NOMPRENOMAR'),
        profession: getElementValue('PROAR'),
        date_entree: getElementValue('DATEHOSPI'),
        heure_entree: getElementValue('HEUREENTREE'),
        date_deces: getElementValue('DINS'),
        heure_deces: getElementValue('HINS'),
        wilaya_deces: getElementValue('WILAYAD'),
        code_p: getElementValue('CODEP'),
        code_c: getElementValue('CODEC'),
        code_n: getElementValue('CODEN'),
        institution: getElementValue('INSTITUTION'),
        clinique: getElementValue('CLINIQUE')
    };

    // Ajouter l'ID du certificat si nous modifions un certificat existant
    // Utiliser la variable globale au lieu de localStorage (qui a déjà été supprimé)
    if (currentCertificateId) {
        data.id = currentCertificateId;
        console.log('📌 ID ajouté aux données du formulaire:', currentCertificateId);
    }

    // Ajouter des valeurs par défaut pour les champs obligatoires si vides
    if (!data.dateDeces) data.dateDeces = '';
    if (!data.heureDeces) data.heureDeces = '';
    if (!data.wilaya_deces) data.wilaya_deces = '';
    if (!data.wilayaResidence) data.wilayaResidence = '';

    return data;
}

// Fonction pour gérer la soumission du formulaire
function handleFormSubmit(e) {
    e.preventDefault();
    const formData = collectFormData();
    // Générer le PDF
    generatePDF(e);
}

// Fonction pour générer le code P
function genererCodeP() {
    // Partie 1 : Génération du codeP
    const WILAYAD = document.getElementById('WILAYAD');
    const NDLMAAP = document.getElementById('NDLMAAP');

    if (WILAYAD && NDLMAAP) {
        const wilaya = WILAYAD.value.trim();
        const ndlmaap = NDLMAAP.value.trim();

        if (wilaya && ndlmaap) {
            const codeP = `${wilaya}${ndlmaap}`;
            const codePField = document.getElementById('CODEP');
            if (codePField) {
                codePField.value = codeP;
            }
            if (typeof remplir1 === 'function') {
                remplir1();
            }
        }
    }

    // Partie 2 : Formatage des champs date et heure
    const dateInputs = document.querySelectorAll('input[type="date"], input[type="time"]');

    dateInputs.forEach(input => {
        const formattedDate = formatDate(input.value);
        if (formattedDate) {
            input.value = formattedDate;
        }
    });
}

function myFunction1() { /* ... */ }
async function initializePDFDocumentForPrint() {
    // Load decesv.pdf template
    const response = await fetch('decesv.pdf');
    if (!response.ok) {
        throw new Error('Failed to load decesv.pdf');
    }
    const pdfBytes = await response.arrayBuffer();
    const pdfDoc = await PDFLib.PDFDocument.load(pdfBytes);

    return {
        pdfDoc,
        page: pdfDoc.getPages()[0],
        font: await pdfDoc.embedFont(PDFLib.StandardFonts.Helvetica)
    };
}

// Gestion PDF
async function generatePDF(e) {
    if (e) e.preventDefault();

    try {
        // Collect form data
        const formData = collectFormData();

        // Initialize PDF document using decesv.pdf template
        const { pdfDoc, page, font } = await initializePDFDocumentForPrint();

        // Draw personal information
        await drawPersonalInfo(page, font, formData);

        // Finalize PDF
        const pdfBytes = await finalizePDF(pdfDoc, formData);

        // Create blob and download
        const blob = new Blob([pdfBytes], { type: 'application/pdf' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = 'print.pdf';
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);

        // Show success message
        alert('Le fichier PDF a été généré avec succès.');
    } catch (error) {
        console.error('Erreur lors de la génération du PDF:', error);
        alert('Erreur lors de la génération du PDF. Veuillez vérifier les données saisies.');
    }
}




function drawDeathInfo(page, font, data) {
    const red = PDFLib.rgb(1, 0, 0);
    const deathDate = formatDate(data.dateDeces);

    // Debug: Afficher les informations de la wilaya de décès
    console.log('Code wilaya de décès:', data.wilayaDeces);

    // Convertir le numéro de wilaya en nom (en français)
    const wilayaDecesName = getWilayaName(data.wilayaDeces, 'fr');
    console.log('Nom wilaya de décès (FR):', wilayaDecesName);

    // Convertir le numéro de wilaya en nom (en arabe pour l'affichage en arabe)
    const wilayaDecesNameAr = getWilayaName(data.wilayaDeces, 'ar');
    console.log('Nom wilaya de décès (AR):', wilayaDecesNameAr);

    // Convertir le numéro de wilaya en nom
    const wilayaResidenceName = getWilayaName(data.wilayaResidence, 'fr');
    const lieuNaissanceName = getWilayaName(data.lieuNaissance, 'fr');

    // Afficher le nom de la wilaya de décès en français aux deux positions
    if (wilayaDecesName) {
        page.drawText(wilayaDecesName, { x: 115, y: 387, font, size: 10, color: red });
        page.drawText(wilayaDecesName, { x: 115, y: 665, font, size: 10, color: red });
    }


    // Informations personnelles
    if (data.nom) page.drawText(data.nom, { x: 60, y: 654, font, size: 10, color: red });
    if (data.prenom) page.drawText(data.prenom, { x: 200, y: 654, font, size: 10, color: red });
    if (data.pere) page.drawText(data.pere, { x: 85, y: 624, font, size: 10, color: red });
    if (data.mere) page.drawText(data.mere, { x: 240, y: 624, font, size: 10, color: red });
    // Écrire le sexe
    if (data.sexe) {
        const sexeCode = getSexeCode(data.sexe);
        page.drawText(sexeCode, { x: 60, y: 635, font, size: 10, color: red });
        page.drawText(sexeCode, { x: 60, y: 342, font, size: 10, color: red });
    }


    // Date et lieu de naissance
    let birthDate = formatDate(data.dateNaissance);

    // Si date présumée est cochée, remplacer par xx/xx/AAAA
    if (data.datePresume && data.dateNaissance) {
        const d = new Date(data.dateNaissance);
        if (!isNaN(d.getTime())) {
            const year = d.getFullYear();
            birthDate = `xx/xx/${year}`;
        }
    }

    if (birthDate) {
        page.drawText(birthDate, { x: 140, y: 612, font, size: 10, color: red });
        page.drawText(birthDate, { x: 120, y: 355, font, size: 10, color: red });
    }

    if (data.communeNaissance) {
        page.drawText(data.communeNaissance, { x: 240, y: 612, font, size: 10, color: red });
    }


    // Date de décès
    if (deathDate) {
        // Positions spécifiées
        page.drawText(deathDate, { x: 280, y: 355, font, size: 10, color: red });
        page.drawText(deathDate, { x: 95, y: 602, font, size: 10, color: red });
        page.drawText(deathDate, { x: 420, y: 638, font, size: 10, color: red });
    }



    // Heure de décès
    if (data.heureDeces) {
        page.drawText(data.heureDeces, { x: 420, y: 627, font, size: 10, color: red });
    }





    // Calculer l'âge
    const ageInfo = calculateAge(data.dateNaissance, data.dateDeces);
    if (ageInfo) {
        // Afficher l'âge en années
        if (ageInfo.years) {
            page.drawText(ageInfo.years, { x: 235, y: 602, font, size: 10, color: red });
            page.drawText(ageInfo.years, { x: 265, y: 345, font, size: 10, color: red });
        }
        // Afficher l'âge en mois
        if (ageInfo.months) {
            page.drawText(ageInfo.months, { x: 308, y: 590, font, size: 10, color: red });
            page.drawText(ageInfo.months, { x: 308, y: 332, font, size: 10, color: red });
        }
        // Afficher l'âge en jours
        if (ageInfo.days) {
            page.drawText(ageInfo.days, { x: 352, y: 590, font, size: 10, color: red });
            page.drawText(ageInfo.days, { x: 352, y: 332, font, size: 10, color: red });
        }
    }


    // Localisation
    if (wilayaDecesName) {
        page.drawText(wilayaDecesName, { x: 115, y: 387, font, size: 10, color: red });
        page.drawText(wilayaDecesName, { x: 115, y: 665, font, size: 10, color: red });
    }
    if (data.wilayaResidence) {
        page.drawText(wilayaResidenceName, { x: 115, y: 365, font, size: 10, color: red });
    }
    if (lieuNaissanceName) {
        page.drawText(lieuNaissanceName, { x: 240, y: 612, font, size: 10, color: red });
    }
    /*  if (data.communeDeces) {
         // page.drawText(data.communeDeces, { x: 115, y: 405, font, size: 10, color: red });
         /* page.drawText(data.communeDeces, { x: 115, y: 683, font, size: 10, color: red });
      } 
      */

    // Lieu de décès
    if (data.lieuDeces) {
        const lieuDeces = data.lieuDeces.toUpperCase();
        const autres = data.autresLieuDeces || '';
        console.log('Valeur de autresLieuDeces:', autres); // Débogage

        switch (lieuDeces) {
            case 'DOM':
                // Domicile
                page.drawText('X', { x: 53, y: 564, font, size: 10, color: red });
                page.drawText('X', { x: 53, y: 305, font, size: 10, color: red });
                break;
            case 'SSPV':
                // Structure privée
                page.drawText('X', { x: 53, y: 555, font, size: 10, color: red });
                page.drawText('X', { x: 53, y: 295, font, size: 10, color: red });
                break;
            case 'SSP':
                // Structure publique
                page.drawText('X', { x: 183, y: 564, font, size: 10, color: red });
                page.drawText('X', { x: 183, y: 305, font, size: 10, color: red });
                break;
            case 'VP':
                // Voie publique
                page.drawText('X', { x: 183, y: 555, font, size: 10, color: red });
                page.drawText('X', { x: 183, y: 295, font, size: 10, color: red });
                break;
            case 'AAP':
                // Autre
                page.drawText('X', { x: 53, y: 543, font, size: 10, color: red });
                page.drawText('X', { x: 53, y: 280, font, size: 10, color: red });
                if (autres) {
                    page.drawText(autres, { x: 150, y: 543, font, size: 10, color: red });
                    page.drawText(autres, { x: 150, y: 280, font, size: 10, color: red });
                } else {
                    // Si le champ est vide, afficher un espace pour éviter les problèmes de formatage
                    page.drawText(' ', { x: 150, y: 543, font, size: 10, color: red });
                    page.drawText(' ', { x: 150, y: 280, font, size: 10, color: red });
                }
                break;
        }
    }
    /* if (data.wilayaDeces) {
         page.drawText(data.wilayaDeces, { x: 115, y: 387, font, size: 10, color: red });
         page.drawText(data.wilayaDeces, { x: 115, y: 665, font, size: 10, color: red });
     }*/


    if (data.communeDeces) {
        page.drawText(data.communeDeces, { x: 115, y: 398, font, size: 10, color: red });
        page.drawText(data.communeDeces, { x: 115, y: 676, font, size: 10, color: red });
    }
    /* if (data.wilayaResidence) {
        page.drawText(data.wilayaResidence, { x: 115, y: 365, font, size: 10, color: red });
    }*/
    if (data.communeResidence) {
        page.drawText(data.communeResidence, { x: 125, y: 375, font, size: 10, color: red });
    }

    // Cause de décès
    if (data.causeDeces !== undefined) {
        const cause = data.causeDeces.toUpperCase();

        // Afficher un X pour les causes CN, CV ou CI
        switch (cause) {

            case 'CN':
                // Cause naturelle
                page.drawText('X', { x: 410, y: 604, font, size: 10, color: red });
                break;
            case 'CV':
                // Cause violente
                page.drawText('X', { x: 410, y: 593, font, size: 10, color: red });
                break;
            case 'CI':
                // Cause indéterminée
                page.drawText('X', { x: 410, y: 583, font, size: 10, color: red });
                break;
            default:
                // Pour les autres valeurs, ne rien afficher
                break;
        }
    }

    // Nature de la mort
    if (data.natureMort) {
        const nature = data.natureMort.toUpperCase();
        switch (nature) {
            case 'NAT':
                // Naturelle
                page.drawText('X', { x: 510, y: 398, font, size: 10, color: red });
                break;
            case 'ACC':
                // Accident
                page.drawText('X', { x: 432, y: 390, font, size: 10, color: red });
                break;
            case 'AID':
                // Auto induite
                page.drawText('X', { x: 487, y: 390, font, size: 10, color: red });
                break;
            case 'AGR':
                // Agression
                page.drawText('X', { x: 433, y: 382, font, size: 10, color: red });
                break;
            case 'IND':
                // Indéterminée
                page.drawText('X', { x: 490, y: 382, font, size: 10, color: red });
                break;
            case 'AAP':
                // Autre
                page.drawText('X', { x: 500, y: 374, font, size: 10, color: red });
                if (data.natureMortAutre) {
                    page.drawText(data.natureMortAutre, { x: 460, y: 374, font, size: 10, color: red });
                }
                break;
        }
    }


    // Affichage du champ placefr en rouge
    page.drawText(data.placefr, {
        x: 414,
        y: 572,
        font,
        size: 10,
        color: red
    });

    // Affichage de la date de signature formatée en rouge aux deux positions
    page.drawText(formatDate(data.DSG), {
        x: 414,
        y: 560,
        font,
        size: 10,
        color: red
    });
    page.drawText(formatDate(data.DSG), {
        x: 60,
        y: 148,
        font,
        size: 10,
        color: red
    });

    // Affichage des éléments de décès maternel
    // Décès maternel
    if (data.DECEMAT) {
        page.drawText('X', {
            x: 482,
            y: 270,
            font,
            size: 10,
            color: red
        });
    } else {
        page.drawText('X', {
            x: 510,
            y: 270,
            font,
            size: 10,
            color: red
        });
    }

    // Durant la grossesse
    if (data.DGRO === 'DGRO') {
        page.drawText('X', {
            x: 446,
            y: 255,
            font,
            size: 10,
            color: red
        });
    } else {
        page.drawText('X', {
            x: 478,
            y: 255,
            font,
            size: 10,
            color: red
        });
    }

    // Accouchement / Avortement
    if (data.DACC === 'DACC') {
        page.drawText('X', {
            x: 414,
            y: 230,
            font,
            size: 10,
            color: red
        });
    } else if (data.DAVO === 'DAVO') {
        page.drawText('X', {
            x: 414,
            y: 230,
            font,
            size: 10,
            color: red
        });
    } else {
        page.drawText('X', {
            x: 445,
            y: 230,
            font,
            size: 10,
            color: red
        });
    }

    // 42 jours après la gestation
    if (data.AGESTATION === 'AGESTATION') {
        page.drawText('X', {
            x: 448,
            y: 215,
            font,
            size: 10,
            color: red
        });
    } else {
        page.drawText('X', {
            x: 475,
            y: 215,
            font,
            size: 10,
            color: red
        });
    }

    // Indéterminé
    if (data.IDETER === 'IDETER') {
        page.drawText('X', {
            x: 449,
            y: 207,
            font,
            size: 10,
            color: red
        });
    }

    // Grossesse multiple
    if (data.GM) {
        page.drawText('X', {
            x: 485,
            y: 351,
            font,
            size: 8,
            color: red
        });
    } else {
        page.drawText('X', {
            x: 512,
            y: 351,
            font,
            size: 8,
            color: red
        });
    }

    // Mort-né
    if (data.MN) {
        page.drawText('X', {
            x: 455,
            y: 343,
            font,
            size: 8,
            color: red
        });
    } else {
        page.drawText('X', {
            x: 485,
            y: 343,
            font,
            size: 8,
            color: red
        });
    }

    // Age gestationnel
    page.drawText(data.AGEGEST, {
        x: 510,
        y: 333,
        font,
        size: 8,
        color: red
    });

    // Poids à la naissance
    page.drawText(data.POIDNSC, {
        x: 518,
        y: 326,
        font,
        size: 8,
        color: red
    });

    // Age de la mère
    page.drawText(data.AGEMERE, {
        x: 490,
        y: 320,
        font,
        size: 8,
        color: red
    });

    // État morbide de la mère
    if (data.DPNAT) {
        page.drawText(data.EMDPNAT, {
            x: 415,
            y: 288,
            font,
            size: 8,
            color: red
        });
    }

    // Causes médicales (Partie I)
    if (data.CIM1) {
        page.drawText(data.CIM1, { x: 140, y: 240, font, size: 10, color: red });
    }
    if (data.CIM2) {
        page.drawText(data.CIM2, { x: 140, y: 220, font, size: 10, color: red });
    }
    if (data.CIM3) {
        page.drawText(data.CIM3, { x: 140, y: 210, font, size: 10, color: red });
    }
    if (data.CIM4) {
        page.drawText(data.CIM4, { x: 140, y: 197, font, size: 10, color: red });
    }

    // Autres états morbides (Partie II)
    if (data.CIM5) {
        page.drawText(data.CIM5, { x: 35, y: 175, font, size: 10, color: red });
    }

    // Cause directe
    if (data.causeDirecte) {
        page.drawText(data.causeDirecte, { x: 140, y: 240, font, size: 10, color: red });
    }
    // État morbide
    if (data.etatMorbide) {
        page.drawText(data.etatMorbide, { x: 140, y: 220, font, size: 10, color: red });
    }
    // Obstacle médico-légal
    if (data.obstacleMedicoLegal) {
        page.drawText('X', { x: 44, y: 481, font, size: 10, color: red });
        page.drawText('X', { x: 482, y: 160, font, size: 10, color: red });
    } else {
        page.drawText('X', { x: 512, y: 160, font, size: 10, color: red });
    }

    // Contamination
    if (data.contamination) {
        page.drawText('X', { x: 303, y: 480, font, size: 10, color: red });
        page.drawText('X', { x: 470, y: 134, font, size: 10, color: red });
    } else {
        page.drawText('X', { x: 505, y: 134, font, size: 10, color: red });
    }

    // Prothèse
    if (data.prothese) {
        page.drawText('X', { x: 44, y: 461, font, size: 10, color: red });
        page.drawText('X', { x: 417, y: 105, font, size: 10, color: red });
    } else {
        page.drawText('X', { x: 453, y: 105, font, size: 10, color: red });
    }

    // Post-opératoire
    if (data.POSTOPP2) {
        page.drawText('X', { x: 448, y: 70, font, size: 10, color: red });
    } else {
        page.drawText('X', { x: 480, y: 70, font, size: 10, color: red });
    }

    if (data.medecin) {
        page.drawText(data.medecin, { x: 420, y: 540, font, size: 10, color: red });
        page.drawText(data.medecin, { x: 288, y: 149, font, size: 10, color: red });
    }
}



async function finalizePDF(pdfDoc, formData) {
    const pdfBytes = await pdfDoc.save();
    const blob = new Blob([pdfBytes], { type: 'application/pdf' });

    // Créer une boîte de dialogue modale pour la sélection du dossier
    const modal = document.createElement('div');
    modal.className = 'fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50';
    modal.innerHTML = `
        <div class="bg-white dark:bg-gray-800 p-6 rounded-lg shadow-xl max-w-md w-full">
            <h3 class="text-lg font-medium text-gray-900 dark:text-white mb-4">Sélectionner le dossier de destination</h3>
            <div class="space-y-4">
                <div class="flex flex-col">
                    <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Nom du fichier:</label>
                    <input type="text" id="fileName" value="certificat_deces_${formData.nom}_${formData.prenom}" 
                           class="form-input rounded-lg bg-white dark:bg-gray-700 border-2 border-gray-300 dark:border-gray-600 px-3 py-2">
                </div>
                <div class="flex flex-col">
                    <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Sélectionner un dossier:</label>
                    <input type="file" id="folderPicker" webkitdirectory directory multiple 
                           class="form-input rounded-lg bg-white dark:bg-gray-700 border-2 border-gray-300 dark:border-gray-600 px-3 py-2">
                </div>
                <div class="flex justify-end space-x-3">
                    <button id="cancelDownload" class="px-4 py-2 bg-gray-200 text-gray-800 rounded hover:bg-gray-300">
                        Annuler
                    </button>
                    <button id="confirmDownload" class="px-4 py-2 bg-primary text-white rounded hover:bg-primary/80">
                        Télécharger
                    </button>
                </div>
            </div>
        </div>
    `;

    document.body.appendChild(modal);

    // Gérer les événements
    return new Promise((resolve, reject) => {
        const fileNameInput = modal.querySelector('#fileName');
        const confirmBtn = modal.querySelector('#confirmDownload');
        const cancelBtn = modal.querySelector('#cancelDownload');

        confirmBtn.addEventListener('click', async () => {
            const fileName = fileNameInput.value.trim() || `certificat_deces_${formData.nom}_${formData.prenom}`;
            const folderPicker = modal.querySelector('#folderPicker');

            // Vérifier si un dossier a été sélectionné
            if (!folderPicker.files.length) {
                alert('Veuillez sélectionner un dossier');
                return;
            }

            // Obtenir le chemin du dossier sélectionné
            const selectedFolder = folderPicker.files[0].webkitRelativePath.split('/')[0];

            // Créer un lien de téléchargement
            const url = URL.createObjectURL(blob);
            const link = document.createElement('a');
            link.href = url;
            link.download = `${fileName}.pdf`;

            // Sauvegarder le fichier dans le dossier sélectionné
            try {
                const response = await fetch(link.href);
                const file = await response.blob();
                const fileHandle = await window.showSaveFilePicker({
                    suggestedName: `${fileName}.pdf`,
                    types: [
                        {
                            description: 'PDF files',
                            accept: { 'application/pdf': ['.pdf'] }
                        }
                    ]
                });

                const writable = await fileHandle.createWritable();
                await writable.write(file);
                await writable.close();

                document.body.removeChild(modal);
                resolve();
            } catch (error) {
                console.error('Erreur lors du téléchargement:', error);
                alert('Erreur lors du téléchargement du fichier');
                reject(error);
            } finally {
                URL.revokeObjectURL(url);
            }
        });

        cancelBtn.addEventListener('click', () => {
            document.body.removeChild(modal);
            reject(new Error('Téléchargement annulé'));
        });
    });
}

function handlePDFError(error) {
    console.error('Erreur de génération:', error);
    alert(`Échec de génération: ${error.message}`);
}

// ============================================================================
// Fonction pour sauvegarder le certificat de décès dans la base de données
// ============================================================================

async function sauvegarderCertificatDeces(formData) {
    console.log('💾 Début de la sauvegarde du certificat de décès...');

    try {
        // Préparer le message pour l'application native
        const message = {
            action: "ajouter_dece",
            nom: formData.nom || '',
            prenom: formData.prenom || '',
            dateNaissance: formData.dateNaissance || '',
            datePresume: formData.datePresume ? 'Oui' : 'Non',
            wilaya_naissance: formData.wilaya_naissance || '',
            sexe: formData.sexe || '',
            pere: formData.pere || '',
            mere: formData.mere || '',
            communeNaissance: formData.communeNaissance || '',
            wilayaResidence: formData.wilayaResidence || '',
            place: formData.place || '',
            placefr: formData.placefr || '',
            DSG: formData.DSG || '',
            DECEMAT: formData.DECEMAT ? 'Oui' : 'Non',
            DGRO: formData.DGRO || '',
            DACC: formData.DACC || '',
            DAVO: formData.DAVO || '',
            AGESTATION: formData.AGESTATION || '',
            IDETER: formData.IDETER || '',
            GM: formData.GM ? 'Oui' : 'Non',
            MN: formData.MN ? 'Oui' : 'Non',
            AGEGEST: formData.AGEGEST || '',
            POIDNSC: formData.POIDNSC || '',
            AGEMERE: formData.AGEMERE || '',
            DPNAT: formData.DPNAT ? 'Oui' : 'Non',
            EMDPNAT: formData.EMDPNAT || '',
            communeResidence: formData.communeResidence || '',
            date_deces: formData.dateDeces || '',
            heure_deces: formData.heureDeces || '',
            lieuDeces: formData.lieuDeces || '',
            autresLieuDeces: formData.autresLieuDeces || '',
            communeDeces: formData.communeDeces || '',
            wilayaDeces: formData.wilayaDeces || '',
            causeDeces: formData.causeDeces || '',
            causeDirecte: formData.causeDirecte || '',
            etatMorbide: formData.etatMorbide || '',
            natureMort: formData.natureMort || '',
            natureMortAutre: formData.natureMortAutre || '',
            obstacleMedicoLegal: formData.obstacleMedicoLegal ? 'Oui' : 'Non',
            contamination: formData.contamination ? 'Oui' : 'Non',
            prothese: formData.prothese ? 'Oui' : 'Non',
            POSTOPP2: formData.POSTOPP2 ? 'Oui' : 'Non',
            CIM1: formData.CIM1 || '',
            CIM2: formData.CIM2 || '',
            CIM3: formData.CIM3 || '',
            CIM4: formData.CIM4 || '',
            CIM5: formData.CIM5 || '',
            nom_ar: formData.nom_ar || '',
            prenom_ar: formData.prenom_ar || '',
            perear: formData.perear || '',
            merear: formData.merear || '',
            lieu_naissance: formData.lieu_naissance || '',
            conjoint: formData.conjoint || '',
            profession: formData.profession || '',
            adresse: formData.adresse || '',
            date_entree: formData.date_entree || '',
            heure_entree: formData.heure_entree || '',
            wilaya_deces: formData.wilaya_deces || '',
            medecin: formData.medecin || '',
            code_p: formData.code_p || '',
            code_c: formData.code_c || '',
            code_n: formData.code_n || ''
        };

console.log('📤 Message à envoyer:', message);

        // Vérifier si l'API native est disponible avant d'essayer de l'utiliser
        const apiNativeDisponible = (typeof browser !== 'undefined' && browser.runtime && browser.runtime.sendNativeMessage) ||
            (typeof chrome !== 'undefined' && chrome.runtime && chrome.runtime.sendNativeMessage);

        if (apiNativeDisponible) {
            console.log('📡 API native détectée, tentative de communication...');
            try {
                await envoyerMessageNatifDece(message);
            } catch (nativeError) {
                console.warn('⚠️ Communication avec API native échouée, utilisation de localStorage:', nativeError.message);
                sauvegarderDansLocalStorage(message);
            }
        } else {
            console.log('💾 API native non disponible, sauvegarde directe dans localStorage');
            // Sauvegarder directement dans localStorage sans essayer l'API native
            sauvegarderDansLocalStorage(message);
        }

    } catch (error) {
        console.error('❌ Erreur lors de la sauvegarde du certificat de décès:', error);
        // Ne pas bloquer le téléchargement du PDF en cas d'erreur de sauvegarde
        console.warn('⚠️ Le PDF a été généré mais la sauvegarde en base de données a échoué');
    }
}

// ============================================================================
// Fonction pour modifier le certificat de décès dans la base de données
// ============================================================================

async function modifierCertificatDeces(formData) {
    console.log('💾 Début de la modification du certificat de décès...');
    console.log('📋 Données reçues:', formData);

    try {
        // Vérifier que l'ID est présent
        if (!formData.id) {
            throw new Error('ID du certificat manquant - impossible de modifier');
        }

        console.log('✅ ID du certificat à modifier:', formData.id);

        // Préparer le message pour l'application native
        const message = {
            action: "modifier_dece",
            data: {
                id: formData.id,
                nom: formData.nom || '',
                prenom: formData.prenom || '',
                dateNaissance: formData.dateNaissance || '',
                datePresume: formData.datePresume ? 'Oui' : 'Non',
                wilaya_naissance: formData.wilaya_naissance || '',
                sexe: formData.sexe || '',
                pere: formData.pere || '',
                mere: formData.mere || '',
                communeNaissance: formData.communeNaissance || '',
                wilayaResidence: formData.wilayaResidence || '',
                place: formData.place || '',
                placefr: formData.placefr || '',
                DSG: formData.DSG || '',
                DECEMAT: formData.DECEMAT ? 'Oui' : 'Non',
                DGRO: formData.DGRO || '',
                DACC: formData.DACC || '',
                DAVO: formData.DAVO || '',
                AGESTATION: formData.AGESTATION || '',
                IDETER: formData.IDETER || '',
                GM: formData.GM ? 'Oui' : 'Non',
                MN: formData.MN ? 'Oui' : 'Non',
                AGEGEST: formData.AGEGEST || '',
                POIDNSC: formData.POIDNSC || '',
                AGEMERE: formData.AGEMERE || '',
                DPNAT: formData.DPNAT ? 'Oui' : 'Non',
                EMDPNAT: formData.EMDPNAT || '',
                communeResidence: formData.communeResidence || '',
                date_deces: formData.dateDeces || '',
                heure_deces: formData.heureDeces || '',
                lieuDeces: formData.lieuDeces || '',
                autresLieuDeces: formData.autresLieuDeces || '',
                communeDeces: formData.communeDeces || '',
                wilayaDeces: formData.wilayaDeces || '',
                causeDeces: formData.causeDeces || '',
                causeDirecte: formData.causeDirecte || '',
                etatMorbide: formData.etatMorbide || '',
                natureMort: formData.natureMort || '',
                natureMortAutre: formData.natureMortAutre || '',
                obstacleMedicoLegal: formData.obstacleMedicoLegal ? 'Oui' : 'Non',
                contamination: formData.contamination ? 'Oui' : 'Non',
                prothese: formData.prothese ? 'Oui' : 'Non',
                POSTOPP2: formData.POSTOPP2 ? 'Oui' : 'Non',
                CIM1: formData.CIM1 || '',
                CIM2: formData.CIM2 || '',
                CIM3: formData.CIM3 || '',
                CIM4: formData.CIM4 || '',
                CIM5: formData.CIM5 || '',
                nom_ar: formData.nom_ar || '',
                prenom_ar: formData.prenom_ar || '',
                perear: formData.perear || '',
                merear: formData.merear || '',
                lieu_naissance: formData.lieu_naissance || '',
                conjoint: formData.conjoint || '',
                profession: formData.profession || '',
                adresse: formData.adresse || '',
                date_entree: formData.date_entree || '',
                heure_entree: formData.heure_entree || '',
                wilaya_deces: formData.wilaya_deces || '',
                medecin: formData.medecin || '',
                code_p: formData.code_p || '',
                code_c: formData.code_c || '',
                code_n: formData.code_n || ''
            }
        };

console.log('📤 Message à envoyer:', message);

        // Vérifier si l'API native est disponible avant d'essayer de l'utiliser
        const apiNativeDisponible = (typeof browser !== 'undefined' && browser.runtime && browser.runtime.sendNativeMessage) ||
            (typeof chrome !== 'undefined' && chrome.runtime && chrome.runtime.sendNativeMessage);

        if (apiNativeDisponible) {
            console.log('📡 API native détectée, tentative de communication...');
            try {
                await envoyerMessageNatifDece(message);
            } catch (nativeError) {
                console.warn('⚠️ Communication avec API native échouée, utilisation de localStorage:', nativeError.message);
                modifierDansLocalStorage(message);
            }
        } else {
            console.log('💾 API native non disponible, modification directe dans localStorage');
            // Modifier directement dans localStorage sans essayer l'API native
            modifierDansLocalStorage(message);
        }

    } catch (error) {
        console.error('❌ Erreur lors de la modification du certificat de décès:', error);
        // Ne pas bloquer le téléchargement du PDF en cas d'erreur de modification
        console.warn('⚠️ Le PDF a été généré mais la modification en base de données a échoué');
    }
}

// Fonction pour envoyer un message à l'application native
async function envoyerMessageNatifDece(message) {
    console.log('📡 Tentative de connexion à l\'application native...');

    // Vérifier les APIs disponibles
    console.log('🔍 APIs disponibles:');
    console.log('- browser:', typeof browser !== 'undefined' ? 'Disponible' : 'Non disponible');
    console.log('- chrome:', typeof chrome !== 'undefined' ? 'Disponible' : 'Non disponible');

    try {
        let response;

        // Essayer avec l'API Firefox d'abord
        if (typeof browser !== 'undefined' && browser.runtime && browser.runtime.sendNativeMessage) {
            console.log('🦊 Utilisation de l\'API Firefox...');
            console.log('🔧 Extension ID:', browser.runtime.id);

            try {
                response = await browser.runtime.sendNativeMessage("com.daoudi.certificat", message);
                console.log('✅ Réponse Firefox:', response);
            } catch (firefoxError) {
                console.error('❌ Erreur Firefox complète:', firefoxError);
                console.error('❌ Type d\'erreur:', typeof firefoxError);
                console.error('❌ Message d\'erreur:', firefoxError.message);
                console.error('❌ Stack trace:', firefoxError.stack || '<empty string>');
                throw firefoxError;
            }
        }
        // Essayer avec l'API Chrome en fallback
        else if (typeof chrome !== 'undefined' && chrome.runtime && chrome.runtime.sendNativeMessage) {
            console.log('🌐 Utilisation de l\'API Chrome...');

            response = await new Promise((resolve, reject) => {
                chrome.runtime.sendNativeMessage("com.daoudi.certificat", message, (response) => {
                    if (chrome.runtime.lastError) {
                        reject(new Error(chrome.runtime.lastError.message));
                    } else {
                        resolve(response);
                    }
                });
            });
            console.log('✅ Réponse Chrome:', response);
        } else {
            throw new Error('Aucune API de messagerie native disponible');
        }

        // Traiter la réponse
        if (response && response.ok) {
            console.log('✅ Certificat de décès sauvegardé avec succès');
            alert('✅ Certificat de décès sauvegardé avec succès dans la base de données !');
        } else {
            const errorMsg = response ? response.error : 'Réponse invalide';
            console.error('❌ Erreur de sauvegarde:', errorMsg);
            alert('❌ Erreur lors de la sauvegarde en base de données: ' + errorMsg);
        }

    } catch (error) {
        // Utiliser console.warn au lieu de console.error pour éviter les messages d'erreur rouges dans la console
        console.warn('⚠️ API de messagerie native non disponible (c\'est normal si vous utilisez la version web):', error.message);
        // Ne pas throw l'erreur pour permettre le fallback localStorage
        throw error;
    }
}

// ============================================================================
// Fonction pour sauvegarder dans localStorage (fallback)
// ============================================================================

function sauvegarderDansLocalStorage(certificat) {
    try {
        console.log('💾 Sauvegarde dans localStorage...');
        
        // Récupérer les certificats existants
        const certificatsExistants = JSON.parse(localStorage.getItem('certificatsDeces') || '[]');
        
        // Ajouter un ID unique et la date de création
        const nouveauCertificat = {
            ...certificat,
            id: Date.now().toString(),
            dateCreation: new Date().toISOString(),
            dateModification: new Date().toISOString()
        };
        
        // Ajouter à la liste
        certificatsExistants.push(nouveauCertificat);
        
        // Sauvegarder dans localStorage
        localStorage.setItem('certificatsDeces', JSON.stringify(certificatsExistants));
        
        console.log('✅ Certificat sauvegardé avec succès dans localStorage');
        console.log('📊 Total des certificats:', certificatsExistants.length);

        // Afficher un message de succès discret (optionnel)
        // alert('✅ Certificat de décès sauvegardé avec succès !');

        return true;
    } catch (error) {
        console.error('❌ Erreur lors de la sauvegarde dans localStorage:', error);
        return false;
    }
}

// ============================================================================
// Fonction pour modifier dans localStorage (fallback)
// ============================================================================

function modifierDansLocalStorage(modifMessage) {
    try {
        console.log('🔄 Modification dans localStorage...');
        
        // Récupérer les certificats existants
        const certificatsExistants = JSON.parse(localStorage.getItem('certificatsDeces') || '[]');
        
        // Trouver le certificat à modifier
        const index = certificatsExistants.findIndex(c => c.id === modifMessage.data.id);
        
        if (index === -1) {
            console.error('❌ Certificat non trouvé avec ID:', modifMessage.data.id);
            return false;
        }
        
        // Mettre à jour le certificat
        certificatsExistants[index] = {
            ...certificatsExistants[index],
            ...modifMessage.data,
            dateModification: new Date().toISOString()
        };
        
        // Sauvegarder dans localStorage
        localStorage.setItem('certificatsDeces', JSON.stringify(certificatsExistants));
        
        console.log('✅ Certificat modifié avec succès dans localStorage');
        console.log('📋 Certificat modifié:', certificatsExistants[index]);
        
        return true;
    } catch (error) {
        console.error('❌ Erreur lors de la modification dans localStorage:', error);
        return false;
    }
}









