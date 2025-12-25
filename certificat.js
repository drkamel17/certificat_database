// Style pour forcer la taille de police des champs date à 11px
const style = document.createElement('style');
style.textContent = `
    input[type="date"] {
        font-size: 11px !important;
    }
`;
document.head.appendChild(style);

// Fonction personnalisée pour conserver les retours à la ligne
function trimPreserveNewlines(text) {
    return text.replace(/^\s+|\s+$/g, '');
}

// Fonction pour sauvegarder les modifications des champs
function sauvegarderModifications() {
    const textInputs = document.querySelectorAll('.full-width-input');
    textInputs.forEach(input => {
        input.addEventListener('input', () => {
            input.setAttribute('value', input.value);
        });
    });
}

// Fonction pour charger les données
function loadData() {
    const polyclinique = localStorage.getItem('polyclinique') || '';
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || '';
    const docteur = localStorage.getItem('docteur') || '';
    
    // Charger les données du patient
    const patientNomPrenom = localStorage.getItem('patientNomPrenom') || '';
    const patientAge = localStorage.getItem('patientAge') || '';
    const patientDateNaissance = localStorage.getItem('patientDateNaissance') || '';
    const dateCertificat = localStorage.getItem('dateCertificat') || '';
    
    document.getElementById('polyclinique').value = polyclinique;
    document.getElementById('polyclinique-ar').value = polycliniqueAr;
    document.getElementById('docteur').value = docteur;
    
    // Remplir les champs du patient
    document.getElementById('patientNomPrenom').value = patientNomPrenom;
    document.getElementById('patientAge').value = patientAge;
    document.getElementById('patientDateNaissance').value = patientDateNaissance;
    document.getElementById('dateCertificat').value = dateCertificat;
    
    // Si aucune date n'est définie, utiliser la date du jour
    if (!dateCertificat) {
        const today = new Date().toISOString().split('T')[0];
        document.getElementById('dateCertificat').value = today;
    }
    
    // Initialiser l'état des boutons de format
    const format = localStorage.getItem('certificatFormat');
    if (format === 'sansEntete') {
        document.getElementById('formatSansEntete').classList.add('selected-format');
    } else {
        // Par défaut, on utilise avec en-tête
        document.getElementById('formatAvecEntete').classList.add('selected-format');
    }
}

// Fonction pour calculer l'âge à partir de la date de naissance
function calculerAge(dateNaissance) {
    if (!dateNaissance) return '';
    
    const today = new Date();
    const birthDate = new Date(dateNaissance);
    
    // Calculer la différence en millisecondes
    const diffTime = Math.abs(today - birthDate);
    const diffDays = Math.floor(diffTime / (1000 * 60 * 60 * 24));
    
    // Si moins de 30 jours, afficher en jours
    if (diffDays < 30) {
        return diffDays + ' jours';
    }
    
    // Si moins de 2 ans, afficher en mois
    if (diffDays < 730) { // ~2 ans
        const diffMonths = Math.floor(diffDays / 30.44);
        return diffMonths + ' mois';
    }
    
    // Sinon, afficher en années
    const diffYears = Math.floor(diffDays / 365.25);
    return diffYears + ' ans';
}

// Fonction pour sauvegarder les données
function saveData() {
    const polyclinique = document.getElementById('polyclinique').value;
    const polycliniqueAr = document.getElementById('polyclinique-ar').value;
    const docteur = document.getElementById('docteur').value;
    
    // Sauvegarder les données du patient
    const patientNomPrenom = document.getElementById('patientNomPrenom').value;
    const patientAge = document.getElementById('patientAge').value;
    const patientDateNaissance = document.getElementById('patientDateNaissance').value;
    const dateCertificat = document.getElementById('dateCertificat').value;
    
    localStorage.setItem('polyclinique', polyclinique);
    localStorage.setItem('polyclinique-ar', polycliniqueAr);
    localStorage.setItem('docteur', docteur);
    
    // Sauvegarder les données du patient
    localStorage.setItem('patientNomPrenom', patientNomPrenom);
    localStorage.setItem('patientAge', patientAge);
    localStorage.setItem('patientDateNaissance', patientDateNaissance);
    localStorage.setItem('dateCertificat', dateCertificat);
    
    alert('Informations sauvegardées avec succès!');
}

// Fonction pour générer l'en-tête
function generateHeader() {
    const polyclinique = localStorage.getItem('polyclinique') || '';
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || '';

    return `
    <div id="head" style="border: 1px solid #000; padding: 10px; margin-bottom: 20px;">
        <table style="width: 100%;">
            <tbody>
                <tr>
                    <td colspan="4">
                        <div style="text-align: center; width: 100%; font-size: 12px;">REPUBLIQUE ALGERIENNE DEMOCRATIQUE ET POPULAIRE</div>
                    </td>
                </tr>
                <tr>
                    <td colspan="4">
                        <div style="text-align: center; width: 100%; font-size: 12px;">MINISTERE DE LA SANTE</div>
                    </td>
                </tr>
                <tr>
                    <td colspan="2">
                        <div style="text-align: left; width: 100%; font-size: 12px; white-space: pre-wrap;">${polyclinique}</div>
                    </td>
                    <td colspan="2" style="text-align: right;">
                        <div style="text-align: right; width: 100%; font-size: 12px; white-space: pre-wrap;" class="arabic-text">${polycliniqueAr}</div>
                    </td>
                </tr>
            </tbody>
        </table>
    </div>
    `;
}

// Fonction pour configurer les gestionnaires d'événements des boutons de format
function setupFormatButtons() {
    const formatAvecEnteteBtn = document.getElementById('formatAvecEntete');
    const formatSansEnteteBtn = document.getElementById('formatSansEntete');
    
    if (formatAvecEnteteBtn) {
        formatAvecEnteteBtn.addEventListener('click', function() {
            // Mettre à jour le localStorage
            localStorage.setItem('certificatFormat', 'avecEntete');
            
            // Mettre à jour l'interface utilisateur
            formatAvecEnteteBtn.classList.add('selected-format');
            formatSansEnteteBtn.classList.remove('selected-format');
        });
    }
    
    if (formatSansEnteteBtn) {
        formatSansEnteteBtn.addEventListener('click', function() {
            // Mettre à jour le localStorage
            localStorage.setItem('certificatFormat', 'sansEntete');
            
            // Mettre à jour l'interface utilisateur
            formatSansEnteteBtn.classList.add('selected-format');
            formatAvecEnteteBtn.classList.remove('selected-format');
        });
    }
}

// Configurer les gestionnaires d'événements lorsque le DOM est chargé
document.addEventListener('DOMContentLoaded', function() {
    loadData();
    setupFormatButtons();
    
    // Ecouteur pour le bouton de catégorie de leishmaniose
    document.getElementById('catLeishmaniose').addEventListener('click', () => {
        console.log("Bouton Catégorie de Leishmaniose cliqué");
        const container = document.getElementById('leishmanioseButtons');
        container.innerHTML = '';

        const buttonInf = document.createElement('button');
        buttonInf.textContent = 'Inférieur ou égal à  3 lésions';
        buttonInf.addEventListener('click', () => {
            ouvrirCertificatLeishmanioseDetail();
        });

        const buttonSup = document.createElement('button');
        buttonSup.textContent = 'Plus de 3 lésions et à  proximité des zones sensibles';
        buttonSup.addEventListener('click', () => {
            catLeishmanioseplus3();
        });

        container.appendChild(buttonInf);
        container.appendChild(buttonSup);
    });

    // Fonction pour gérer le clic sur le bouton Catégorie Anti-Rabique
    // Cette fonction rend visibles les boutons classe02, classe03 et prex
    function genererCatAntiRabique() {
        const classe02 = document.getElementById('classe02');
        const classe03 = document.getElementById('classe03');
        const prex = document.getElementById('prex');
        
        // Supprimer la classe 'hidden' pour les rendre visibles
        if (classe02) classe02.classList.remove('hidden');
        if (classe03) classe03.classList.remove('hidden');
        if (prex) prex.classList.remove('hidden');
    }

    // Ecouteur pour le bouton Catégorie Anti-Rabique
    // This will make the classe02, classe03, and prex buttons visible when clicked
    document.getElementById('genererCatAntiRabique').addEventListener('click', genererCatAntiRabique);

    // Ecouteur pour le bouton de requisition
    document.getElementById('requisition').addEventListener('click', function () {
        const modal = document.createElement('div');
        modal.className = 'modal';
        modal.innerHTML = `
        <div class="modal-content">
            <h3>Requisition Médicale</h3>
            <div class="info barcode" style="height: 80px;">
                <svg id="barcode" data-numero="${patientInfo.numero || ''}" style="height: 100%;"></svg>
            </div>
            <div class="button-group">
				 <button class="modal-button" id="requisitionApte">Apte pour garde à  vue</button>
				 <button class="modal-button" id="requisitionInapte">Inapte pour garde à  vue</button>
				
            </div>
        </div>
    `;

        document.body.appendChild(modal);
        // Ecouteur pour le bouton requisitionApte
        document.querySelector('#requisitionApte').addEventListener('click', () => {
            requisitionApte(); // Ouvre la modale de choix Zagreb ou Essens
        });
        // Ecouteur pour le bouton requisitionInapte
        document.querySelector('#requisitionInapte').addEventListener('click', () => {
            requisitionInapte(); // Appelle la fonction Tissulairesanssar
        });

        // Ajouter un écouteur de clic pour fermer la modale
        modal.addEventListener('click', function (event) {
            // Si l'utilisateur clique en dehors du contenu de la modale
            if (event.target === modal) {
                modal.remove();
                // Rafraîchir la page
                window.location.reload();
            }
        });

        // Ajouter le style pour la popup
        const style = document.createElement('style');
        style.textContent = `
        .modal {
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background-color: rgba(0, 0, 0, 0.5);
            display: flex;
            justify-content: center;
            align-items: center;
            z-index: 1000;
        }
        .modal-content {
            background-color: white;
            padding: 20px;
            border-radius: 5px;
            box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
            text-align: center;
        }
        .button-group {
            display: flex;
            gap: 10px;
            justify-content: center;
            margin-top: 20px;
        }
        .modal-button {
            padding: 10px 20px;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 16px;
        }
        .modal-button:first-child {
            background-color: #4CAF50;
            color: white;
            z-index: 1000;
        }
        .modal-content {
            background-color: white;
            padding: 20px;
            border-radius: 8px;
            max-width: 400px;
            width: 90%;
            text-align: center;
        }
        .button-group {
            display: flex;
            gap: 10px;
            margin-top: 20px;
        }
        .button-group button {
            flex: 1;
            padding: 12px;
            border: none;
            border-radius: 5px;
            cursor: pointer;
            font-size: 16px;
            transition: background-color 0.3s;
        }
        .button-group button:hover {
            background-color: #f0f0f0;
        }
    `;
        document.head.appendChild(style);
    });

    // Ecouteurs pour les classes
    document.getElementById('classe02').addEventListener('click', function () {
        ouvrirModalClasse02();
    });

    document.getElementById('classe03').addEventListener('click', function () {
        ouvrirModalClasse03();
    });

    document.getElementById('prex').addEventListener('click', function () {
        ouvrirModalPrex();
    });
});

// Fonction pour générer un certificat d'éviction scolaire
function genererCertificat() {
    const polyclinique = document.getElementById('polyclinique').value;
    const docteur = document.getElementById('docteur').value;
    
    // Get patient information from the form fields
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '[Nom de l\'élève]';
    const patientAge = document.getElementById('patientAge').value;
    const patientDateNaissance = document.getElementById('patientDateNaissance').value;
    const dateCertificat = document.getElementById('dateCertificat').value || new Date().toISOString().split('T')[0];
    
    // Construire la partie de l'âge/date de naissance
    let ageInfo = '';
    if (patientAge) {
        ageInfo = 'né(e) le ' + patientAge;
    } else if (patientDateNaissance) {
        ageInfo = 'né(e) le ' + patientDateNaissance;
    } else {
        ageInfo = 'né(e) le [Date de naissance]';
    }
    
    // Format the date for display
    const formattedDate = new Date(dateCertificat).toLocaleDateString('fr-FR');
    
    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';
    
    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"></div>';
    }
    
    const certificatHtml = `
<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Certificat médical d'éviction scolaire</title>
<style>
body {
font-family: Arial, sans-serif;
padding: 20px;
background-color: #f9f9f9;
}
.certificat {
background-color: white;
border: 1px solid #ddd;
padding: 20px 40px;
max-width: 600px;
margin: 0 auto;
margin-top: 60px;
box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
padding-top: 30px;
}
h1 {
text-align: center;
color: #333;
text-decoration: underline;
font-size: 20px;
margin-top: 15px;
margin-bottom: 25px;
}
p {
line-height: 1.5;
color: #555;
}
.editable-field {
border-bottom: 1px dashed #666;
display: inline-block;
min-width: 50px;
min-height: 20px;
padding: 2px 4px;
margin: 0 3px;
}
.editable-area {
border: 1px solid #ddd;
border-radius: 4px;
padding: 8px;
margin: 5px 0;
width: 100%;
min-height: 20px;
resize: vertical;
overflow: hidden;
font-family: inherit;
font-size: inherit;
line-height: inherit;
}
.editable-area:focus {
outline: none;
border-color: #007bff;
}
#head {
margin-bottom: 20px;
}
#head table {
width: 100%;
border: 0px solid #000000;
padding: 4px;
margin-bottom: 15px;
}
#head td {
text-align: center;
}
.print-button {
text-align: center;
margin-top: 20px;
}
.print-button button {
padding: 10px 20px;
font-size: 16px;
background-color: #007bff;
color: white;
border: none;
border-radius: 5px;
cursor: pointer;
}
.print-button button:hover {
background-color: #0056b3;
}
@media print {
@page {
    size: A5;
    margin: 0.2cm 0.2cm 0.2cm 0.2cm;
}

body {
    margin: 0 !important;
    padding: 0 !important;
    font-size: 10pt !important;
    line-height: 1.2 !important;
    background-color: white;
}

.certificat {
    border: none;
    box-shadow: none;
    margin: 0 !important;
    padding: 2px 8px !important;
    max-width: 100% !important;
}

h1 {
    font-size: 14pt !important;
    margin: 5px 0 !important;
    margin-top: 2cm !important;
}

input[type="text"],
input[type="date"],
textarea {
    border: none !important;
    background: none !important;
    box-shadow: none !important;
    outline: none !important;
    font-size: 9pt !important;
}

input[type="text"]:focus,
input[type="date"]:focus,
textarea:focus {
    border: none !important;
    outline: none !important;

}

.print-button {
    display: none;
}

.editable-field, .editable-area {
    border: none !important;
}

/* Additional space optimization */
* {
    margin-top: 0 !important;
    margin-bottom: 2px !important;
}

p {
    margin: 2px 0 !important;
    font-size: 9pt !important;
}
}
</style>
</head>
<body>
${enteteContent}
<div class="certificat">
<h1>Certificat médical d'éviction scolaire</h1>
<br><br><br>
<p>
Je soussigné, Dr <input type="text" id="docteur" value="${docteur}" placeholder="">, certifie avoir examiné ce jour
<strong><input type="text" value="${patientNomPrenom}" style="width: 150px;"></strong>,
<span class="editable-field" contenteditable="true" style="min-width: 100px; display: inline-block;">${ageInfo}</span>.
</p>
<p>
déclare que son état de santé nécessite une éviction scolaire de
<input type="text" placeholder="1 (un)" style="width: 80px;"> Jour(s)
à daté du <span class="editable-field" contenteditable="true" style="min-width: 120px; display: inline-block;">${formattedDate}</span> <br>
sauf complication.
</p>
<p>
<textarea placeholder=" " style="width: 450px;"></textarea>
</p>
<p style="text-align: right; margin-top: 30px;">
Dont certificat&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp<br>
<span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp;
</p>
</div>
    <div class="print-button" style="display: flex; align-items: center; justify-content: center; gap: 15px;">
    <div style="display: flex; align-items: center; gap: 8px;">
        <label for="fontSize" style="font-size: 14px; margin: 0;">Taille du texte:</label>
        <input type="number" id="fontSize" min="8" max="20" value="14" style="width: 60px; padding: 5px; border: 1px solid #bdbdbd; border-radius: 4px;">
    </div>
    <button id="printButton">Imprimer le Certificat</button>
</div>
<script src="print.js"></script>
<script src="certificat-unified-font-size.js"></script>
<script>
    // Appliquer la taille de police sauvegardée et masquer les éléments non désirés à l'impression
    document.addEventListener('DOMContentLoaded', () => {
        const savedFontSize = localStorage.getItem('certificatFontSize') || '14';
        const styleElement = document.createElement('style');
        styleElement.textContent = "@media print { .print-button { display: none !important; } }";
        styleElement.id = 'certificatFontSizeStyle';
        document.head.appendChild(styleElement);
        
        // Add print functionality
        document.getElementById('printButton').addEventListener('click', function() {
            window.print();
        });
    });
</script>
</body>
</html>
    `;

    var newWindow = window.open("", "_blank");
    if (newWindow) {
        // Stocker la référence de la fenêtre pour le script de sauvegarde
        window.lastOpenedWindow = newWindow;

        // Définir manuellement window.opener pour permettre la communication
        try {
            Object.defineProperty(newWindow, 'opener', {
                value: window,
                writable: false,
                configurable: false
            });
        } catch (e) {
            // Si cela échoue, stocker la référence ailleurs
            newWindow._parentWindow = window;
        }

        newWindow.document.write(certificatHtml);
        newWindow.document.close();

        // Fonction pour effectuer la sauvegarde
        function effectuerSauvegarde(nombreJoursValue) {
            // Récupérer les données depuis les champs de la popup
            const docteurInput = newWindow.document.getElementById('docteur');
            const medecin = docteurInput ? docteurInput.value.trim() : '';

            // Récupérer le nom et prénom
            const nomPrenomInput = newWindow.document.querySelector('strong input[type="text"]');
            let nom = '', prenom = '';
            if (nomPrenomInput && nomPrenomInput.value) {
                const parts = nomPrenomInput.value.trim().split(' ');
                if (parts.length >= 2) {
                    nom = parts[0];
                    prenom = parts.slice(1).join(' ');
                }
            }

            // Vérifier les données
            if (!nom || !prenom) {
                afficherMessageDansPopup('Erreur: Nom et prénom du patient requis.', 'error');
                return;
            }

            if (!medecin) {
                afficherMessageDansPopup('Erreur: Nom du médecin requis.', 'error');
                return;
            }

            // Récupérer la date de naissance depuis le champ editable-field
            const editableFields = newWindow.document.querySelectorAll('.editable-field');
            let dateNaissance = '';
            for (let field of editableFields) {
                const text = field.textContent || field.innerText || '';
                const parentText = field.parentElement ? field.parentElement.textContent || '' : '';
                if (parentText.includes('né(e)') || parentText.includes('né') || parentText.includes('née')) {
                    dateNaissance = text.trim();
                    break;
                }
            }

            if (!dateNaissance && editableFields.length > 0) {
                dateNaissance = (editableFields[0].textContent || editableFields[0].innerText || '').trim();
            }

            // Date du certificat
            const today = new Date();
            const dateCertificat = today.toISOString().split('T')[0];

            // Préparer le message
            const message = {
                action: "ajouter_arret_travail",
                nom: nom,
                prenom: prenom,
                medecin: medecin,
                nombre_jours: parseInt(nombreJoursValue),
                date_certificat: dateCertificat,
                date_naissance: dateNaissance || null
            };

            console.log('📤 Message à envoyer:', message);

            // Utiliser directement la fonction stockée dans la popup
            const sauvegarderFn = window.sauvegarderArretTravailDepuisPopup;

            if (sauvegarderFn && typeof sauvegarderFn === 'function') {
                sauvegarderFn(message).then(response => {
                    if (response && response.success) {  // Changé de response.ok à response.success
                        afficherMessageDansPopup('Arrêt de travail sauvegardé avec succès !', 'success');
                    } else {
                        const errorMsg = response ? response.error : 'Réponse invalide';
                        afficherMessageDansPopup('Erreur lors de la sauvegarde: ' + errorMsg, 'error');
                    }
                }).catch(error => {
                    console.error('❌ Erreur lors de la sauvegarde:', error);
                    afficherMessageDansPopup('Erreur lors de la sauvegarde: ' + error.message, 'error');
                });
            } else {
                setTimeout(() => {
                    const fn = window.sauvegarderArretTravailDepuisPopup;
                    if (fn && typeof fn === 'function') {
                        fn(message).then(response => {
                            if (response && response.success) {  // Changé de response.ok à response.success
                                afficherMessageDansPopup('Arrêt de travail sauvegardé avec succès !', 'success');
                            } else {
                                const errorMsg = response ? response.error : 'Réponse invalide';
                                afficherMessageDansPopup('Erreur lors de la sauvegarde: ' + errorMsg, 'error');
                            }
                        }).catch(error => {
                            console.error('❌ Erreur lors de la sauvegarde:', error);
                            afficherMessageDansPopup('Erreur lors de la sauvegarde: ' + error.message, 'error');
                        });
                    } else {
                        afficherMessageDansPopup('Erreur: Fonction de sauvegarde non disponible. Rechargez la page et réessayez.', 'error');
                    }
                }, 500);
            }
        }

        // Attacher l'événement d'impression directement après la fermeture du document
        newWindow.onload = function () {
            const printButton = newWindow.document.getElementById('printButton');
            if (printButton) {
                const newPrintButton = printButton.cloneNode(true);
                printButton.parentNode.replaceChild(newPrintButton, printButton);

                newPrintButton.addEventListener('click', function () {
                    const joursInputs = newWindow.document.querySelectorAll('input[type="text"]');
                    let nombreJours = '';
                    for (let input of joursInputs) {
                        const parentText = input.parentElement ? input.parentElement.textContent : '';
                        if (parentText.includes('Jour(s)') || parentText.includes('arret de travail')) {
                            nombreJours = input.value.trim();
                            break;
                        }
                    }

                    if (nombreJours) {
                        effectuerSauvegarde(nombreJours);
                    }

                    setTimeout(() => {
                        newWindow.print();
                    }, 500);
                });
            }
        };

        // Fonction pour afficher un message personnalisé dans la popup
        function afficherMessageDansPopup(message, type = 'info') {
            if (newWindow && !newWindow.closed) {
                newWindow.focus();
                newWindow.alert(message);
            }
        }

        // Ajouter le bouton de sauvegarde après que le document soit chargé
        function ajouterBoutonSauvegarde() {
            try {
                const printButton = newWindow.document.getElementById('printButton');
                const saveButton = newWindow.document.getElementById('sauvegarderArretPopup');

                if (printButton && !saveButton) {
                    console.log('✅ Ajout du bouton de sauvegarde dans la popup');

                    const boutonSauvegarde = newWindow.document.createElement('button');
                    boutonSauvegarde.id = 'sauvegarderArretPopup';
                    boutonSauvegarde.innerHTML = '💾 Sauvegarder Arrêt';
                    boutonSauvegarde.style.cssText = 'background-color: #28a745; color: white; border: none; padding: 10px 20px; margin-left: 15px; border-radius: 5px; cursor: pointer; font-size: 16px; transition: background-color 0.3s;';

                    boutonSauvegarde.addEventListener('mouseenter', function () {
                        this.style.backgroundColor = '#218838';
                    });
                    boutonSauvegarde.addEventListener('mouseleave', function () {
                        this.style.backgroundColor = '#28a745';
                    });

                    printButton.parentNode.appendChild(boutonSauvegarde);

                    boutonSauvegarde.addEventListener('click', function () {
                        console.log('💾 Clic sur le bouton de sauvegarde');

                        const joursInputs = newWindow.document.querySelectorAll('input[type="text"]');
                        let nombreJours = '';
                        for (let input of joursInputs) {
                            const parentText = input.parentElement ? input.parentElement.textContent : '';
                            if (parentText.includes('Jour(s)') || parentText.includes('arret de travail')) {
                                nombreJours = input.value.trim();
                                break;
                            }
                        }

                        if (!nombreJours) {
                            const promptDiv = newWindow.document.createElement('div');
                            promptDiv.id = 'promptNombreJours';
                            promptDiv.style.cssText = 'position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%); z-index: 10001; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3); min-width: 300px;';
                            promptDiv.innerHTML = '<div style="margin-bottom: 15px; font-weight: bold; font-size: 16px;">Nombre de jours d\'arrêt de travail</div><input type="number" id="inputNombreJours" value="1" min="1" style="width: 100%; padding: 8px; border: 1px solid #ccc; border-radius: 4px; font-size: 14px; margin-bottom: 15px;"><div style="display: flex; gap: 10px; justify-content: flex-end;"><button id="btnPromptOk" style="padding: 8px 20px; background: #28a745; color: white; border: none; border-radius: 4px; cursor: pointer;">OK</button><button id="btnPromptCancel" style="padding: 8px 20px; background: #6c757d; color: white; border: none; border-radius: 4px; cursor: pointer;">Annuler</button></div>';

                            const overlay = newWindow.document.createElement('div');
                            overlay.id = 'promptOverlay';
                            overlay.style.cssText = 'position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0, 0, 0, 0.5); z-index: 10000;';

                            newWindow.document.body.appendChild(overlay);
                            newWindow.document.body.appendChild(promptDiv);

                            setTimeout(() => {
                                const input = newWindow.document.getElementById('inputNombreJours');
                                if (input) {
                                    input.focus();
                                    input.select();
                                }
                            }, 100);

                            const btnOk = newWindow.document.getElementById('btnPromptOk');
                            const btnCancel = newWindow.document.getElementById('btnPromptCancel');

                            const cleanup = () => {
                                if (promptDiv.parentNode) promptDiv.parentNode.removeChild(promptDiv);
                                if (overlay.parentNode) overlay.parentNode.removeChild(overlay);
                            };

                            btnOk.addEventListener('click', () => {
                                const value = newWindow.document.getElementById('inputNombreJours').value;
                                cleanup();
                                if (value && !isNaN(value) && parseInt(value) > 0) {
                                    effectuerSauvegarde(value);
                                } else {
                                    afficherMessageDansPopup('Veuillez entrer un nombre de jours valide', 'warning');
                                }
                            });

                            btnCancel.addEventListener('click', () => {
                                cleanup();
                            });

                            const input = newWindow.document.getElementById('inputNombreJours');
                            input.addEventListener('keydown', (e) => {
                                if (e.key === 'Enter') {
                                    btnOk.click();
                                } else if (e.key === 'Escape') {
                                    btnCancel.click();
                                }
                            });
                        } else {
                            if (!nombreJours || isNaN(nombreJours) || parseInt(nombreJours) <= 0) {
                                afficherMessageDansPopup('Veuillez entrer un nombre de jours valide', 'warning');
                                return;
                            }
                            effectuerSauvegarde(nombreJours);
                        }
                    });

                    console.log('✅ Bouton de sauvegarde ajouté avec succès');
                } else if (!printButton) {
                    console.log('⏳ Bouton printButton pas encore disponible, réessai...');
                    setTimeout(ajouterBoutonSauvegarde, 100);
                } else if (saveButton) {
                    console.log('ℹ️ Bouton de sauvegarde déjà présent');
                }
            } catch (e) {
                console.error('❌ Erreur lors de l\'ajout du bouton:', e);
            }
        }

        // Créer une fonction de sauvegarde dans la fenêtre parent
        const sauvegarderFn = async function (message) {
            console.log('🔗 Sauvegarde depuis popup, message:', message);
            
            // Implémentation de la vraie logique de sauvegarde en base de données
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

        // Stocker la fonction de sauvegarde
        window.sauvegarderArretTravailDepuisPopup = sauvegarderFn;

        // Ajouter le bouton de sauvegarde après un léger délai
        setTimeout(ajouterBoutonSauvegarde, 200);
    } else {
        console.log("Popup bloquée par le navigateur.");
    }
}

// Fonction pour générer un certificat d'inaptitude sportive
function inaptitudeSport() {
    // Ajouter le style pour déplacer le titre lors de l'impression
    const style = document.createElement('style');
    style.textContent = `
    @media print {
      .certificat h1 {
        margin-top: 2cm !important;
      }
    }
  `;
    document.head.appendChild(style);

    // Get patient information from the form fields
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '[Nom du patient]';
    const patientAge = document.getElementById('patientAge').value;
    const patientDateNaissance = document.getElementById('patientDateNaissance').value;
    const dateCertificat = document.getElementById('dateCertificat').value || new Date().toISOString().split('T')[0];
    
    const polyclinique = localStorage.getItem('polyclinique') || "";
    const docteur = document.getElementById('docteur').value || localStorage.getItem('docteur') || "";

    // Construire la partie de l'âge/date de naissance
    let ageInfo = '';
    if (patientAge) {
        ageInfo = patientAge;
    } else if (patientDateNaissance) {
        ageInfo = patientDateNaissance;
    } else {
        ageInfo = '[Date de naissance]';
    }
    
    // Format the date for display
    const formattedDate = new Date(dateCertificat).toLocaleDateString('fr-FR');

    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';

    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"></div>';
    }

    const certificatHtml = `
    <!DOCTYPE html>
    <html lang="fr">
    <head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CERTIFICAT MEDICAL D'INAPTITUDE AU SPORT</title>
    <style>
    body {
        font-family: Arial, sans-serif;
        padding: 20px;
        background-color: #f9f9f9;
    }
    .certificat {
        background-color: white;
        border: 1px solid #ddd;
        padding: 20px;
        max-width: 600px;
        margin: 0 auto;
        margin-top: 60px;
        box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
    }
    h1 {
        text-align: center;
        color: #333;
        text-decoration: underline;
        font-size: 14pt !important;
        margin: 5px 0 !important;
    }
    h2 {
        text-align: center;
        color: #555;
        font-size: 16px;
        margin-top: 5px;
        margin-bottom: 15px;
    }
    p {
        line-height: 1.5;
        color: #555;
    }
	
    .editable-field {
        border-bottom: 1px dashed #666;
        display: inline-block;
        min-width: 50px;
        min-height: 20px;
        padding: 2px 4px;
        margin: 0 3px;
    }
    .editable-area {
        border: 1px solid #ddd;
        border-radius: 4px;
        padding: 8px;
        margin: 5px 0;
        width: 100%;
        min-height: 20px;
        resize: vertical;
        overflow: hidden;
        font-family: inherit;
        font-size: inherit;
        line-height: inherit;
    }
    .editable-area:focus {
        outline: none;
        border-color: #007bff;
    }
    #head {
        margin-bottom: 20px;
    }
    #head table {
        width: 100%;
        border: 0px solid #000000;
        padding: 4px;
        margin-bottom: 15px;
    }
    #head td {
        text-align: center;
    }
    .print-button {
        text-align: center;
        margin-top: 20px;
    }
    .print-button button {
        padding: 10px 20px;
        font-size: 16px;
        background-color: #007bff;
        color: white;
        border: none;
        border-radius: 5px;
        cursor: pointer;
    }
    .print-button button:hover {
        background-color: #0056b3;
    }
    @media print {
        @page {
            size: A5;
            margin: 0.2cm 0.2cm 0.2cm 0.2cm;
        }

        body {
            margin: 0 !important;
            padding: 0 !important;
            font-size: 10pt !important;
            line-height: 1.2 !important;
            background-color: white;
        }

        .certificat {
            border: none;
            box-shadow: none;
            margin: 0 !important;
            padding: 2px 8px !important;
            max-width: 100% !important;
        }

        h1 {
            font-size: 14pt !important;
            margin: 5px 0 !important;
            margin-top: 2cm !important;
        }

        input[type="text"],
        input[type="date"],
        textarea {
            border: none !important;
            background: none !important;
            box-shadow: none !important;
            outline: none !important;
            font-size: 9pt !important;
        }

        input[type="text"]:focus,
        input[type="date"]:focus,
        textarea:focus {
            border: none !important;
            outline: none !important;
        }

        /* Rendre le placeholder transparent lors de l'impression */
        input::placeholder,
        textarea::placeholder {
            color: transparent; /* Masquer le placeholder */
        }

/* Styles existants */
.print-button {
                display: none;
            }

            /* Masquer les contrôles d'impression et de sauvegarde */
            .print-button div[style*="align-items: center"],
            .print-button button {
                display: none !important;
            }

            /* Masquer les contrôles d'impression et de sauvegarde */
            .print-button div[style*="align-items: center"],
            .print-button button {
                display: none !important;
            }

/* Masquer les contrôles d'impression et de sauvegarde */
.print-button div[style*="align-items: center"],
.print-button button {
    display: none !important;
}
        .editable-field, .editable-area {
            border: none !important;
        }

        /* Additional space optimization */
        * {
            margin-top: 0 !important;
            margin-bottom: 2px !important;
        }

        p {
            margin: 2px 0 !important;
            font-size: 9pt !important;
        }
        
        .contenu-certificat {
            margin-top: 1.5cm !important;
        }
        
    }
    </style>
    </head>
    <body>
    ${enteteContent}
  <div class="certificat">
    <h1>CERTIFICAT MEDICAL D'INAPTITUDE AU SPORT</h1>
    <div class="contenu-certificat" style="margin-top: 1.5cm !important;">
      <p>
        Je soussigné(e), Dr <input type="text" id="docteur" value="${docteur}" placeholder="Medecin">, certifie avoir examiné ce jour :<br>
    <strong><input type="text" value="${patientNomPrenom}" style="width: 180px;"></strong>
    <br>
    née le : <strong><input type="text" value="${ageInfo}" style="width: 120px;"></strong>
    <br> met en évidence des contre-indications à  la pratique de sports<br>
        <textarea class="editable-area" style="width: 90%;" placeholder=" "></textarea>
      </p>
      <p style="text-align: right;">
        Signature :<br>
        <span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
      </p>
    </div>
    <div class="print-button" style="display: flex; align-items: center; justify-content: center; gap: 15px;">
        <div style="display: flex; align-items: center; gap: 8px;">
            <label for="fontSize" style="font-size: 14px; margin: 0;">Taille du texte:</label>
            <input type="number" id="fontSize" min="8" max="20" value="14" style="width: 60px; padding: 5px; border: 1px solid #bdbdbd; border-radius: 4px;">
        </div>
        <button id="printButton">Imprimer le Certificat</button>
    </div>
    <script>
    // Sauvegarder les modifications dans le localStorage
    function sauvegarderModifications() {
        const polycliniqueInput = document.getElementById('polyclinique');
        const polycliniqueArInput = document.getElementById('polyclinique-ar');
        const docteurInput = document.getElementById('docteur');
        
        if (polycliniqueInput && polycliniqueInput.value) {
            localStorage.setItem('polyclinique', polycliniqueInput.value);
        }
        
        if (polycliniqueArInput && polycliniqueArInput.value) {
            localStorage.setItem('polyclinique-ar', polycliniqueArInput.value);
        }
        
        if (docteurInput && docteurInput.value) {
            localStorage.setItem('docteur', docteurInput.value);
        }
    }

    // Ecouteur pour le bouton d'impression
    document.getElementById('printButton').addEventListener('click', function() {
        sauvegarderModifications();
        window.print();
    });
    </script>
    </body>
    </html>
    `;

    // Ouvrir le certificat dans un nouvel onglet
    const newTab = window.open('', '_blank');
    newTab.document.write(certificatHtml);
    newTab.document.close();
}

// Fonction pour générer une justification de présence
function justification() {
    // Get patient information from the form fields
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '[Nom du patient]';
    const patientAge = document.getElementById('patientAge').value;
    const patientDateNaissance = document.getElementById('patientDateNaissance').value;
    const dateCertificat = document.getElementById('dateCertificat').value || new Date().toISOString().split('T')[0];
    
    const polyclinique = localStorage.getItem('polyclinique') || "";
    const docteur = document.getElementById('docteur').value || localStorage.getItem('docteur') || "";
    
    // Construire la partie de l'âge/date de naissance
    let ageInfo = '';
    if (patientAge) {
        ageInfo = 'né(e) le ' + patientAge;
    } else if (patientDateNaissance) {
        ageInfo = 'né(e) le ' + patientDateNaissance;
    } else {
        ageInfo = 'né(e) le [Date de naissance]';
    }
    
    // Format the date for display
    const formattedDate = new Date(dateCertificat).toLocaleDateString('fr-FR');
    
    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';
    
    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"></div>';
    }
    
    const certificatHtml = `
<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Justification</title>
    <style>
body {
font-family: Arial, sans-serif;
padding: 20px;
background-color: #f9f9f9;
}
.certificat {
background-color: white;
border: 1px solid #ddd;
padding: 20px;
max-width: 600px;
margin: 0 auto;
margin-top: 60px;
box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
}
h1 {
text-align: center;
color: #333;
text-decoration: underline;
font-size: 20px;
}
p {
line-height: 1.5;
color: #555;
}
.editable-field {
border-bottom: 1px dashed #666;
display: inline-block;
min-width: 50px;
min-height: 20px;
padding: 2px 4px;
margin: 0 3px;
}
.editable-area {
border: 1px solid #ddd;
border-radius: 4px;
padding: 8px;
margin: 5px 0;
width: 100%;
min-height: 20px;
resize: vertical;
overflow: hidden;
font-family: inherit;
font-size: inherit;
line-height: inherit;
}
.editable-area:focus {
outline: none;
border-color: #007bff;
}
#head {
margin-bottom: 20px;
}
#head table {
width: 100%;
border: 0px solid #000000;
padding: 4px;
margin-bottom: 15px;
}
#head td {
text-align: center;
}
.print-button {
text-align: center;
margin-top: 20px;
}
.print-button button {
padding: 10px 20px;
font-size: 16px;
background-color: #007bff;
color: white;
border: none;
border-radius: 5px;
cursor: pointer;
}
.print-button button:hover {
background-color: #0056b3;
}
#head .arabic-text {
font-size: 12px;
text-align: right;
}
#head div {
font-size: 12px;
white-space: pre-wrap;
text-align: left;
}
#head td:first-child {
text-align: left !important;
padding-left: 0 !important;
margin-left: 0 !important;
}
@media print {
@page {
    size: A5;
    margin: 0.2cm 0.2cm 0.2cm 0.2cm;
}

body {
    margin: 0 !important;
    padding: 0 !important;
    font-size: 10pt !important;
    line-height: 1.2 !important;
    background-color: white;
}

.certificat {
    border: none;
    box-shadow: none;
    margin: 0 !important;
    padding: 2px 8px !important;
    max-width: 100% !important;
}

h1 {
    font-size: 14pt !important;
    margin: 5px 0 !important;
    margin-top: 2cm !important;
}

input[type="text"],
input[type="date"],
textarea {
    border: none !important;
    background: none !important;
    box-shadow: none !important;
    outline: none !important;
    font-size: 9pt !important;
}

input[type="text"]:focus,
input[type="date"]:focus,
textarea:focus {
    border: none !important;
    outline: none !important;
}


/* Styles existants */
.print-button {
    display: none;
}

/* Masquer les contrôles d'impression et de sauvegarde */
.print-button div[style*="align-items: center"],
.print-button button {
    display: none !important;
}
.editable-field, .editable-area {
    border: none !important;
}

/* Additional space optimization */
* {
    margin-top: 0 !important;
    margin-bottom: 2px !important;
}

p {
    margin: 2px 0 !important;
    font-size: 9pt !important;
}
}
</style>
</head>
<body>
${enteteContent}
    <div class="certificat">
        <h1>Justification de présence</h1>
        <div class="contenu-certificat" style="margin-top: 1.5cm !important;">
        <p>
            Je soussigné, Dr <input type="text" id="docteur" value="${docteur}" placeholder="" style="width: 120px;">, 
            certifie avoir examiné ce jour le(la) susnommé(e) 
            <strong><input type="text" value="${patientNomPrenom}" style="width: 180px;"></strong>,
            <span class="editable-field" contenteditable="true" style="min-width: 100px; display: inline-block;">${ageInfo}</span>.
        </p>
        <p>
            est présenter pour la consultation ce jour le 
             <span class="editable-field" contenteditable="true" style="min-width: 120px; display: inline-block;">${formattedDate}</span> à  l'heure:<input type="time" style="font-size: 11px !important;"> <br> 
            <div style="direction: rtl; text-align: right;">
                 تم فحص المعني يوم :   <span class="editable-field" contenteditable="true" style="min-width: 120px; display: inline-block; margin-left: 10px;">${formattedDate}</span>   <br>
                على الساعة : <input type="time" > 
            </div>
        </p>
        <p style="text-align: right; margin-top: 30px;">
            Dont certificat<br>
            <span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>
        </p>
    </div>
  <div class="print-button" style="display: flex; align-items: center; justify-content: center; gap: 15px;">
        <div style="display: flex; align-items: center; gap: 8px;">
            <label for="fontSize" style="font-size: 14px; margin: 0;">Taille du texte:</label>
            <input type="number" id="fontSize" min="8" max="20" value="14" style="width: 60px; padding: 5px; border: 1px solid #bdbdbd; border-radius: 4px;">
        </div>
        <button id="printButton">Imprimer le Certificat</button>
    </div>
    <script>
        document.addEventListener('DOMContentLoaded', function() {
            const printButton = document.getElementById('printButton');
            if (printButton) {
                printButton.addEventListener('click', function () {
                    window.print();
                });
            }
        });
    </script>
</body>
</html>
    `;
    
    const newWindow = window.open();
    newWindow.document.write(certificatHtml);
    newWindow.document.close();
}

// Fonction pour générer un certificat de non-grossesse
function genererNonGrossesse() {
    const { nom, prenom, dob } = patientInfo;

    const today = new Date();
    const year = today.getFullYear();
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const day = String(today.getDate()).padStart(2, '0');
    const todayFormatted = `${year}-${month}-${day}`;

    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";
    const docteur = localStorage.getItem('docteur') || "";

    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';

    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"></div>';
    }

    const certificatContent = `
    <!DOCTYPE html>
    <html lang="fr">
    <head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Certificat de Non-Grossesse</title>
    <style>
    body {
        font-family: Arial, sans-serif;
        padding: 20px;
        background-color: #f9f9f9;
    }
    .certificat {
        background-color: white;
        border: 1px solid #ddd;
        padding: 20px;
        max-width: 600px;
        margin: 0 auto;
        margin-top: 60px;
        box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
    }
    h1 {
        text-align: center;
        color: #333;
        text-decoration: underline;
        font-size: 20px;
    }
    h2 {
        text-align: center;
        color: #555;
        font-size: 16px;
        margin-top: 5px;
        margin-bottom: 15px;
    }
    p {
        line-height: 1.5;
        color: #555;
    }
    .editable-field {
        border-bottom: 1px dashed #666;
        display: inline-block;
        min-width: 50px;
        min-height: 20px;
        padding: 2px 4px;
        margin: 0 3px;
    }
    .editable-area {
        border: 1px solid #ddd;
        border-radius: 4px;
        padding: 8px;
        margin: 5px 0;
        width: 100%;
        min-height: 20px;
        resize: vertical;
        overflow: hidden;
        font-family: inherit;
        font-size: inherit;
        line-height: inherit;
    }
    .editable-area:focus {
        outline: none;
        border-color: #007bff;
    }
    #head {
        margin-bottom: 20px;
    }
    #head table {
        width: 100%;
        border: 0px solid #000000;
        padding: 4px;
        margin-bottom: 15px;
    }
    #head td {
        text-align: center;
    }
    .print-button {
        text-align: center;
        margin-top: 20px;
    }
    .print-button button {
        padding: 10px 20px;
        font-size: 16px;
        background-color: #007bff;
        color: white;
        border: none;
        border-radius: 5px;
        cursor: pointer;
    }
    .print-button button:hover {
        background-color: #0056b3;
    }
    @media print {
        @page {
            size: A5;
            margin: 0.2cm 0.2cm 0.2cm 0.2cm;
        }
        body {
            margin: 0 !important;
            padding: 0 !important;
            font-size: 10pt !important;
            background-color: white;
        }
        .certificat {
            padding: 2px 8px !important;
            max-width: 100% !important;
            border: none;
            box-shadow: none;
            margin-top: 0;
        }
        h1 {
            font-size: 14pt !important;
            margin: 5px 0 !important;
            margin-top: 2cm !important;
        }
        h2 {
            font-size: 12pt !important;
            margin: 3px 0 !important;
        }
        p {
            font-size: 9pt !important;
            margin: 2px 0 !important;
            line-height: 1.2 !important;
        }
        input[type="text"],
        input[type="date"],
        textarea {
            border: none !important;
            background: none !important;
            box-shadow: none !important;
            outline: none !important;
            font-size: 9pt !important;
        }
        input[type="text"]:focus,
        input[type="date"]:focus,
        textarea:focus {
            border: none !important;
            outline: none !important;
        }
        /* Rendre le placeholder transparent lors de l'impression */
        input::placeholder,
        textarea::placeholder {
            color: transparent;
}
.print-button {
display: none;
}

/* Masquer les contrôles d'impression et de sauvegarde */
.print-button div[style*="align-items: center"],
.print-button button {
    display: none !important;
}

.editable-field, .editable-area {
border: none !important;
}
}

        /* Masquer les contrôles d'impression et de sauvegarde */
        .print-button div[style*="align-items: center"],
        .print-button button {
            display: none !important;
        }
        
        .editable-field, .editable-area {
            border: none !important;
        }
    }
    </style>
    </head>
    <body>
    ${enteteContent}
    <div class="certificat">
    <h1>CERTIFICAT MEDICAL DE NON-GROSSESSE</h1>
    <div class="contenu-certificat" style="margin-top: 1cm !important;">
    <p>
    Je soussigné(e), Dr <input type="text" id="docteur" value="${docteur}" placeholder="Medecin">, certifie avoir examiné ce jour :<br>
    <strong><input type="text" value="${nom} ${prenom}" style="width: 180px;"></strong>
    <br>
    née le : <strong><input type="text" value="${dob}" style="width: 120px;"></strong>
    <br>
    Je n'ai constaté aucun signe clinique évocateur d'une grossesse en cours à  la date du présent certificat.<br>
    Ce certificat est délivré à  la demande de l'intéressée et remis en main propre pour servir et valoir ce que de droit.<br>
    </p>
    <p style="text-align: right;">Signature :<br>
    <span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
    </p>
    </div>
    </div>
    <div class="print-button" style="display: flex; align-items: center; justify-content: center; gap: 15px;">
        <div style="display: flex; align-items: center; gap: 8px;">
            <label for="fontSize" style="font-size: 14px; margin: 0;">Taille du texte:</label>
            <input type="number" id="fontSize" min="8" max="20" value="14" style="width: 60px; padding: 5px; border: 1px solid #bdbdbd; border-radius: 4px;">
        </div>
        <button id="printButton">Imprimer le Certificat</button>
    </div>
    <script src="print.js"></script>
    <script src="certificat-unified-font-size.js"></script>
    <script>
    // Sauvegarder les modifications dans le localStorage
    function sauvegarderModifications() {
        const polycliniqueInput = document.getElementById('polyclinique');
        const polycliniqueArInput = document.getElementById('polyclinique-ar');
        const docteurInput = document.getElementById('docteur');
        
        if (polycliniqueInput && polycliniqueInput.value) {
            localStorage.setItem('polyclinique', polycliniqueInput.value);
        }
        
        if (polycliniqueArInput && polycliniqueArInput.value) {
            localStorage.setItem('polyclinique-ar', polycliniqueArInput.value);
        }
        
        if (docteurInput && docteurInput.value) {
            localStorage.setItem('docteur', docteurInput.value);
        }
    }

    // Ecouteur pour le bouton d'impression
    document.getElementById('printButton').addEventListener('click', function() {
        sauvegarderModifications();
        window.print();
    });
    </script>
    </body>
    </html>
    `;

    // Ouvrir le certificat dans un nouvel onglet
    const newTab = window.open('', '_blank');
    newTab.document.write(certificatContent);
    newTab.document.close();
}

// Fonction pour générer un certificat de maladie chronique
function genererChronique() {
    const polyclinique = document.getElementById('polyclinique').value;
    const docteur = document.getElementById('docteur').value;
    
    // Get patient information from the form fields
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '[Nom du patient]';
    const patientAge = document.getElementById('patientAge').value;
    const patientDateNaissance = document.getElementById('patientDateNaissance').value;
    const dateCertificat = document.getElementById('dateCertificat').value || new Date().toISOString().split('T')[0];
    
    // Construire la partie de l'âge/date de naissance
    let ageInfo = '';
    if (patientAge) {
        ageInfo = 'âgé(e) de ' + patientAge;
    } else if (patientDateNaissance) {
        ageInfo = 'né(e) le ' + patientDateNaissance;
    } else {
        ageInfo = 'né(e) le [Date de naissance]';
    }
    
    // Format the date for display
    const formattedDate = new Date(dateCertificat).toLocaleDateString('fr-FR');
    
    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';
    
    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"></div>';
    }
    
    const certificatHtml = `
<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Certificat de Maladie Chronique</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            padding: 20px;
            background-color: #f9f9f9;
        }
        .certificat {
            background-color: white;
            border: 1px solid #ddd;
            padding: 20px;
            max-width: 600px;
            margin: 0 auto;
            margin-top: 60px;
            box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
        }
        h1 {
            text-align: center;
            color: #333;
            text-decoration: underline;
            font-size: 20px;
        }
        p {
            line-height: 1.5;
            color: #555;
        }
        .signature {
            text-align: right;
            margin-top: 50px;
        }
        @media print {
            body {
                background-color: white;
            }
            .certificat {
                border: none;
                box-shadow: none;
                margin-top: 0;
            }
            .print-button {
                display: none;
            }
        }
        .print-button {
            text-align: center;
            margin-top: 20px;
        }
        .print-button button {
            padding: 10px 20px;
            font-size: 16px;
            background-color: #007bff;
            color: white;
            border: none;
            border-radius: 5px;
            cursor: pointer;
        }
        .print-button button:hover {
            background-color: #0056b3;
        }
        #head {
            margin-bottom: 20px;
        }
        #head table {
            width: 100%;
            border: 0px solid #000000;
            padding: 4px;
            margin-bottom: 15px;
        }
        #head td {
            text-align: center;
        }
    </style>
</head>
<body>
    ${enteteContent}
    <div class="certificat">
        <h1>CERTIFICAT DE MALADIE CHRONIQUE</h1>
        <p>
            Je soussigné(e), Dr <strong>${docteur || '[Nom du docteur]'}</strong>, certifie avoir examiné 
            le patient <strong>${patientNomPrenom}</strong>, ${ageInfo}.
        </p>
        <p>
            À la suite de cet examen, je constate que le patient souffre d'une maladie chronique nécessitant 
            un suivi médical régulier et des soins continus.
        </p>
        <p>
            Cette affection chronique nécessite des consultations périodiques et un traitement adapté.
        </p>
        <div class="signature">
            Fait à <strong>${polyclinique || '[Nom de la polyclinique]'}</strong>, le <strong>${formattedDate}</strong><br><br><br>
            Signature et cachet<br>
            Dr <strong>${docteur || '[Nom du docteur]'}</strong>
        </div>
    </div>
    <div class="print-button">
        <button onclick="window.print()">Imprimer le certificat</button>
    </div>
</body>
</html>
    `;
    
    const newWindow = window.open();
    newWindow.document.write(certificatHtml);
    newWindow.document.close();
}

// Fonction pour générer un certificat de prolongation d'arrêt de travail
function genererProlongation() {
    // Get patient information from the form fields
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '[Nom du patient]';
    const patientAge = document.getElementById('patientAge').value;
    const patientDateNaissance = document.getElementById('patientDateNaissance').value;
    const dateCertificat = document.getElementById('dateCertificat').value || new Date().toISOString().split('T')[0];
    
    // Split patient name into first and last name
    let nom = '';
    let prenom = '';
    if (patientNomPrenom) {
        const parts = patientNomPrenom.split(' ');
        nom = parts[0] || '';
        prenom = parts.slice(1).join(' ') || '';
    }
    
    // Use date of birth if available
    const dob = patientDateNaissance || '[Date de naissance]';
    
    const today = new Date();
    const year = today.getFullYear();
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const day = String(today.getDate()).padStart(2, '0');
    const todayFormatted = `${year}-${month}-${day}`;

    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";
    const docteur = localStorage.getItem('docteur') || "";

    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';

    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"></div>';
    }

    const certificatContent = `
<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="Content-Security-Policy" content="script-src 'self' 'unsafe-inline' 'unsafe-eval'; object-src 'none';">
    <title>Certificat de prolongation d'arret de Travail</title>
    <style>
body {
font-family: Arial, sans-serif;
padding: 20px;
background-color: #f9f9f9;
}
.certificat {
background-color: white;
border: 1px solid #ddd;
padding: 20px;
max-width: 600px;
margin: 0 auto;
margin-top: 60px;
box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
}
h1 {
text-align: center;
color: #333;
text-decoration: underline;
font-size: 20px;
}
p {
line-height: 1.5;
color: #555;
}
.editable-field {
border-bottom: 1px dashed #666;
display: inline-block;
min-width: 50px;
min-height: 20px;
padding: 2px 4px;
margin: 0 3px;
}
.editable-area {
border: 1px solid #ddd;
border-radius: 4px;
padding: 8px;
margin: 5px 0;
width: 100%;
min-height: 20px;
resize: vertical;
overflow: hidden;
font-family: inherit;
font-size: inherit;
line-height: inherit;
}
.editable-area:focus {
outline: none;
border-color: #007bff;
}
#head {
margin-bottom: 20px;
}
#head table {
width: 100%;
border: 0px solid #000000;
padding: 4px;
margin-bottom: 15px;
}
#head td {
text-align: center;
}
.print-button {
text-align: center;
margin-top: 20px;
}
.print-button button {
padding: 10px 20px;
font-size: 16px;
background-color: #007bff;
color: white;
border: none;
border-radius: 5px;
cursor: pointer;
}
.print-button button:hover {
background-color: #0056b3;
}
@media print {
@page {
    size: A5;
    margin: 0.2cm 0.2cm 0.2cm 0.2cm;
}

body {
    margin: 0 !important;
    padding: 0 !important;
    font-size: 10pt !important;
    line-height: 1.2 !important;
    background-color: white;
}

.certificat {
    border: none;
    box-shadow: none;
    margin: 0 !important;
    padding: 2px 8px !important;
    max-width: 100% !important;
}

h1 {
    font-size: 14pt !important;
    margin: 5px 0 !important;
    margin-top: 2cm !important;
}

input[type="text"],
input[type="date"],
textarea {
    border: none !important;
    background: none !important;
    box-shadow: none !important;
    outline: none !important;
    font-size: 9pt !important;
}

input[type="text"]:focus,
input[type="date"]:focus,
textarea:focus {
    border: none !important;
    outline: none !important;
}


/* Styles existants */
.print-button {
    display: none;
}

/* Masquer les contrôles d'impression et de sauvegarde */
.print-button div[style*="align-items: center"],
.print-button button {
    display: none !important;
}
.editable-field, .editable-area {
    border: none !important;
}

/* Additional space optimization */
* {
    margin-top: 0 !important;
    margin-bottom: 2px !important;
}

p {
    margin: 2px 0 !important;
    font-size: 9pt !important;
}
}
</style>
</head>
<body>
${enteteContent}
    <div class="certificat">
        <h1>certificat de prolongation d'arret de travail</h1>
        <div class="contenu-certificat" style="margin-top: 1.5cm !important;">
        <p>
            Je soussigné, Dr <input type="text" id="docteur" value="${docteur}" placeholder="" style="width: 120px;">, 
            certifie avoir examiné ce jour le(la) susnommé(e) 
            <strong><input type="text" value="${nom} ${prenom}" style="width: 180px;"></strong>,
            <span class="editable-field" contenteditable="true" style="min-width: 100px; display: inline-block;">né(e) le ${dob}</span>.
        </p>
        <p>
            déclare que son état de santé nécessite une prolongation d'arret de travail de 
            <input type="text" placeholder="1 (un)" style="width: 70px;"> Jour(s)
            à  dater du <span class="editable-field" contenteditable="true" style="min-width: 120px; display: inline-block;">${todayFormatted}</span> sauf complication.
        </p>
        <p style="text-align: right; margin-top: 30px;">
            Dont certificat<br>
            <span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>
        </p>
    </div>
    <div class="print-button" style="display: flex; align-items: center; justify-content: center; gap: 15px;">
        <div style="display: flex; align-items: center; gap: 8px;">
            <label for="fontSize" style="font-size: 14px; margin: 0;">Taille du texte:</label>
            <input type="number" id="fontSize" min="8" max="20" value="14" style="width: 60px; padding: 5px; border: 1px solid #bdbdbd; border-radius: 4px;">
        </div>
        <button id="printButton">Imprimer le Certificat</button>
		<button id="sauvegarderProl">sauvegarder</button>
    </div>
<script>
document.addEventListener('DOMContentLoaded', function() {
    // Fonction pour imprimer le certificat
    document.getElementById('printButton').addEventListener('click', function() {
        window.print();
    });
    
    // Fonction pour ajuster la taille du texte
    document.getElementById('fontSize').addEventListener('change', function() {
        const fontSize = this.value;
        const style = document.createElement('style');
        style.textContent = '@media print { body { font-size: ' + fontSize + 'pt !important; } }';
        document.head.appendChild(style);
    });
});
</script>
</body>
</html>
    `;

var newWindow = window.open("", "_blank");
    if (newWindow) {
        // Stocker la référence de la fenêtre pour le script de sauvegarde
        window.lastOpenedWindow = newWindow;

        // Définir manuellement window.opener pour permettre la communication
        try {
            Object.defineProperty(newWindow, 'opener', {
                value: window,
                writable: false,
                configurable: false
            });
        } catch (e) {
            // Si cela échoue, stocker la référence ailleurs
            newWindow._parentWindow = window;
        }

        newWindow.document.write(certificatContent);
        newWindow.document.close();

        // Fonction pour effectuer la sauvegarde de prolongation
        function effectuerSauvegardeProlongation(nombreJoursValue) {
            // Récupérer les données depuis les champs de la popup
            const docteurInput = newWindow.document.getElementById('docteur');
            const medecin = docteurInput ? docteurInput.value.trim() : '';

            // Récupérer le nom et prénom
            const nomPrenomInput = newWindow.document.querySelector('strong input[type="text"]');
            let nom = '', prenom = '';
            if (nomPrenomInput && nomPrenomInput.value) {
                const parts = nomPrenomInput.value.trim().split(' ');
                if (parts.length >= 2) {
                    nom = parts[0];
                    prenom = parts.slice(1).join(' ');
                }
            }

            // Vérifier les données
            if (!nom || !prenom) {
                alert('Erreur: Nom et prénom du patient requis.');
                return;
            }

            if (!medecin) {
                alert('Erreur: Nom du médecin requis.');
                return;
            }

            // Récupérer la date de naissance depuis le champ editable-field
            const editableFields = newWindow.document.querySelectorAll('.editable-field');
            let dateNaissance = '';
            for (let field of editableFields) {
                const text = field.textContent || field.innerText || '';
                const parentText = field.parentElement ? field.parentElement.textContent || '' : '';
                if (parentText.includes('né(e)') || parentText.includes('né') || parentText.includes('née')) {
                    dateNaissance = text.trim();
                    break;
                }
            }

            // Date du certificat
            const today = new Date();
            const dateCertificat = today.toISOString().split('T')[0];

            // Préparer le message
            const message = {
                action: "ajouter_prolongation",
                nom: nom,
                prenom: prenom,
                medecin: medecin,
                nombre_jours: parseInt(nombreJoursValue),
                date_certificat: dateCertificat,
                date_naissance: dateNaissance || null
            };

            console.log('Message prolongation à envoyer:', message);

            // Utiliser directement la fonction fetch de la fenêtre parente avec URL complète
            fetch('http://localhost:5000/api/ajouter_prolongation', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(message)
            })
            .then(response => response.json())
            .then(data => {
                // Afficher les messages dans la fenêtre popup
                if (newWindow && !newWindow.closed) {
                    if (data && data.success) {
                        newWindow.alert('Prolongation d\'arrêt de travail sauvegardée avec succès !');
                    } else {
                        const errorMsg = data ? data.error : 'Réponse invalide';
                        newWindow.alert('Erreur lors de la sauvegarde: ' + errorMsg);
                    }
                }
            })
            .catch(error => {
                console.error('❌ Erreur lors de la sauvegarde:', error);
                
                // Afficher les messages dans la fenêtre popup
                if (newWindow && !newWindow.closed) {
                    // Fallback: Afficher les données pour copie manuelle si l'API n'est pas accessible
                    if (error.message.includes('Failed to fetch') || error.message.includes('CORS')) {
                        const fallbackMessage = `
API non accessible. Veuillez démarrer le serveur avec: python api_simple.py
Ou copiez ces données manuellement:

${JSON.stringify(message, null, 2)}
                        `;
                        newWindow.alert(fallbackMessage);
                    } else {
                        newWindow.alert('Erreur lors de la sauvegarde: ' + error.message);
                    }
                }
            });
        }

        // Rendre la fonction accessible globalement
        window.sauvegarderProlongationDepuisPopup = effectuerSauvegardeProlongation;

        // Attacher l'événement de sauvegarde après le chargement de la fenêtre
        newWindow.onload = function () {
            const sauvegarderButton = newWindow.document.getElementById('sauvegarderProl');
            if (sauvegarderButton) {
                const newSauvegarderButton = sauvegarderButton.cloneNode(true);
                sauvegarderButton.parentNode.replaceChild(newSauvegarderButton, sauvegarderButton);

                newSauvegarderButton.addEventListener('click', function () {
                    // Récupérer le nombre de jours
                    const allInputs = newWindow.document.querySelectorAll('input[type="text"]');
                    let nombreJours = '';
                    for (let input of allInputs) {
                        const parentText = input.parentElement ? input.parentElement.textContent : '';
                        if (parentText.includes('Jour(s)') || parentText.includes('prolongation')) {
                            nombreJours = input.value.trim();
                            break;
                        }
                    }

                    if (!nombreJours) {
                        alert('Erreur: Nombre de jours requis.');
                        return;
                    }

                    // Appeler la fonction de sauvegarde
                    effectuerSauvegardeProlongation(nombreJours);
                });
            }
        };
    } else {
        console.log("Popup bloquée par le navigateur.");
    }
}

// Fonction pour générer une lettre médicale
function genererLettre() {
    const polyclinique = document.getElementById('polyclinique').value;
    const docteur = document.getElementById('docteur').value;
    
    // Get patient information from the form fields
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '[Nom du patient]';
    const patientAge = document.getElementById('patientAge').value;
    const patientDateNaissance = document.getElementById('patientDateNaissance').value;
    const dateCertificat = document.getElementById('dateCertificat').value || new Date().toISOString().split('T')[0];
    
    // Construire la partie de l'âge/date de naissance
    let ageInfo = '';
    if (patientAge) {
        ageInfo = 'âgé(e) de ' + patientAge;
    } else if (patientDateNaissance) {
        ageInfo = 'né(e) le ' + patientDateNaissance;
    } else {
        ageInfo = 'né(e) le [Date de naissance]';
    }
    
    // Format the date for display
    const formattedDate = new Date(dateCertificat).toLocaleDateString('fr-FR');
    
    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';
    
    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"></div>';
    }
    
    const certificatHtml = `
<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Lettre Médicale</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            padding: 20px;
            background-color: #f9f9f9;
        }
        .certificat {
            background-color: white;
            border: 1px solid #ddd;
            padding: 20px;
            max-width: 600px;
            margin: 0 auto;
            margin-top: 60px;
            box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
        }
        h1 {
            text-align: center;
            color: #333;
            text-decoration: underline;
            font-size: 20px;
        }
        p {
            line-height: 1.5;
            color: #555;
        }
        .signature {
            text-align: right;
            margin-top: 50px;
        }
        @media print {
            body {
                background-color: white;
            }
            .certificat {
                border: none;
                box-shadow: none;
                margin-top: 0;
            }
            .print-button {
                display: none;
            }
        }
        .print-button {
            text-align: center;
            margin-top: 20px;
        }
        .print-button button {
            padding: 10px 20px;
            font-size: 16px;
            background-color: #007bff;
            color: white;
            border: none;
            border-radius: 5px;
            cursor: pointer;
        }
        .print-button button:hover {
            background-color: #0056b3;
        }
        #head {
            margin-bottom: 20px;
        }
        #head table {
            width: 100%;
            border: 0px solid #000000;
            padding: 4px;
            margin-bottom: 15px;
        }
        #head td {
            text-align: center;
        }
    </style>
</head>
<body>
    ${enteteContent}
    <div class="certificat">
        <h1>LETTRE MEDICALE</h1>
        <p>
            Je soussigné(e), Dr <strong>${docteur || '[Nom du docteur]'}</strong>, certifie avoir examiné ce jour :
            <strong>${patientNomPrenom}</strong>, ${ageInfo}.
        </p>
        <p>
            Je constate que l'état de santé de ce patient nécessite des soins médicaux réguliers.
        </p>
        <p>
            Ce certificat est délivré à la demande de l'intéressée et remis en main propre pour servir et valoir ce que de droit.
        </p>
    </div>
    <div class="print-button">
        <button onclick="window.print()">Imprimer le certificat</button>
    </div>
</body>
</html>
    `;
    
    const newWindow = window.open();
    newWindow.document.write(certificatHtml);
    newWindow.document.close();
}

// Fonction pour générer une attestation de décès
function genererDeces() {
    // Get patient information from the form fields
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '[Nom du patient]';
    const patientAge = document.getElementById('patientAge').value;
    const patientDateNaissance = document.getElementById('patientDateNaissance').value;
    const patientDateDeces = document.getElementById('dateCertificat').value || new Date().toISOString().split('T')[0];
    
    const polyclinique = localStorage.getItem('polyclinique') || "";
    const docteur = localStorage.getItem('docteur') || "";

    // Construire la partie de l'âge/date de naissance
    let ageInfo = '';
    if (patientAge) {
        ageInfo = 'âgé(e) de ' + patientAge;
    } else if (patientDateNaissance) {
        ageInfo = 'né(e) le ' + patientDateNaissance;
    } else {
        ageInfo = 'né(e) le [Date de naissance]';
    }
    
    // Format the date for display
    const formattedDate = new Date(patientDateDeces).toLocaleDateString('fr-FR');
    
    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';
    
    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"></div>';
    }
    
    const certificatHtml = `
<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Attestation de Décès</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            padding: 20px;
            background-color: #f9f9f9;
        }
        .certificat {
            background-color: white;
            border: 1px solid #ddd;
            padding: 20px;
            max-width: 600px;
            margin: 0 auto;
            margin-top: 60px;
            box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
        }
        h1 {
            text-align: center;
            color: #333;
            text-decoration: underline;
            font-size: 20px;
        }
        p {
            line-height: 1.5;
            color: #555;
        }
        .signature {
            text-align: right;
            margin-top: 50px;
        }
        @media print {
            body {
                background-color: white;
            }
            .certificat {
                border: none;
                box-shadow: none;
                margin-top: 0;
            }
            .print-button {
                display: none;
            }
        }
        .print-button {
            text-align: center;
            margin-top: 20px;
        }
        .print-button button {
            padding: 10px 20px;
            font-size: 16px;
            background-color: #007bff;
            color: white;
            border: none;
            border-radius: 5px;
            cursor: pointer;
        }
        .print-button button:hover {
            background-color: #0056b3;
        }
        #head {
            margin-bottom: 20px;
        }
        #head table {
            width: 100%;
            border: 0px solid #000000;
            padding: 4px;
            margin-bottom: 15px;
        }
        #head td {
            text-align: center;
        }
    </style>
</head>
<body>
    ${enteteContent}
    <div class="certificat">
        <h1>ATTESTATION DE DÉCÈS</h1>
        <p>
            Je soussigné(e), Dr <strong>${docteur || '[Nom du docteur]'}</strong>, certifie avoir examiné ce jour :
            <strong>${patientNomPrenom}</strong>, ${ageInfo}.
        </p>
        <p>
            Je certifie que le décès de ce patient s'est produit le <strong>${formattedDate}</strong>.
        </p>
        <div class="signature">
            Fait à <strong>${polyclinique || '[Nom de la polyclinique]'}</strong>, le <strong>${formattedDate}</strong><br><br><br>
            Signature et cachet<br>
            Dr <strong>${docteur || '[Nom du docteur]'}</strong>
        </div>
    </div>
    <div class="print-button">
        <button onclick="window.print()">Imprimer le certificat</button>
    </div>
</body>
</html>
    `;
    
    const newWindow = window.open();
    newWindow.document.write(certificatHtml);
    newWindow.document.close();
}

// Fonction pour générer un certificat de reprise de travail
function genererReprise() {
    const { nom, prenom, dob } = patientInfo;


    const today = new Date();
    const year = today.getFullYear();
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const day = String(today.getDate()).padStart(2, '0');
    const todayFormatted = `${year}-${month}-${day}`;

    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";
    const docteur = localStorage.getItem('docteur') || "";


    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';

    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"></div>';
    }



    const certificatContent = `
<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>reprise de travail</title>
<style>
body {
font-family: Arial, sans-serif;
padding: 20px;
background-color: #f9f9f9;
}
.certificat {
background-color: white;
border: 1px solid #ddd;
padding: 20px;
max-width: 600px;
margin: 0 auto;
margin-top: 60px;
box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
}
h1 {
text-align: center;
color: #333;
text-decoration: underline;
font-size: 20px;
}
p {
line-height: 1.5;
color: #555;
}
.editable-field {
border-bottom: 1px dashed #666;
display: inline-block;
min-width: 50px;
min-height: 20px;
padding: 2px 4px;
margin: 0 3px;
}
.editable-area {
border: 1px solid #ddd;
border-radius: 4px;
padding: 8px;
margin: 5px 0;
width: 100%;
min-height: 20px;
resize: vertical;
overflow: hidden;
font-family: inherit;
font-size: inherit;
line-height: inherit;
}
.editable-area:focus {
outline: none;
border-color: #007bff;
}
#head {
margin-bottom: 20px;
}
#head table {
width: 100%;
border: 0px solid #000000;
padding: 4px;
margin-bottom: 15px;
}
#head td {
text-align: center;
}
.print-button {
text-align: center;
margin-top: 20px;
}
.print-button button {
padding: 10px 20px;
font-size: 16px;
background-color: #007bff;
color: white;
border: none;
border-radius: 5px;
cursor: pointer;
}
.print-button button:hover {
background-color: #0056b3;
}
@media print {
@page {
size: A5;
margin: 0.2cm 0.2cm 0.2cm 0.2cm;
}
body {
margin: 0 !important;
padding: 0 !important;
font-size: 10pt !important;
background-color: white;
}
.certificat {
padding: 2px 8px !important;
max-width: 100% !important;
border: none;
box-shadow: none;
margin-top: 0;
}
h1 {
font-size: 14pt !important;
margin: 5px 0 !important;
margin-top: 2cm !important;
}
p {
font-size: 9pt !important;
margin: 2px 0 !important;
line-height: 1.2 !important;
}
input[type="text"],
input[type="date"],
textarea {
border: none !important;
background: none !important;
box-shadow: none !important;
outline: none !important;
font-size: 9pt !important;
}
input[type="text"]:focus,
input[type="date"]:focus,
textarea:focus {
border: none !important;
outline: none !important;
}
.print-button {
display: none;
}
.editable-field, .editable-area {
border: none !important;
}
}
</style>
</head>
<body>
${enteteContent}
<div class="certificat">
<h1>Certificat médical de reprise de travail</h1>
<br><br><br>
<p>
Je soussigné, Dr <input type="text" id="docteur" value="${docteur}" placeholder="">, certifie avoir examiné ce jour
le(a) nommé(e) <strong><input type="text" value="${nom} ${prenom}" style="width: 180px;"></strong>,
<span class="editable-field" contenteditable="true" style="min-width: 100px; display: inline-block;">né(e) le ${dob}</span>.<br>
Déclare que son état de santé lui permet de reprendre son travail le : <span class="editable-field" contenteditable="true" style="min-width: 120px; display: inline-block;">${todayFormatted}</span><br>
Sauf complication.<br>

</p>
<p style="text-align: right;">DONT CERTIFICAT<br>
<span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp;
</p>
</div>
<div class="print-button" style="display: flex; align-items: center; justify-content: center; gap: 15px;">
    <div style="display: flex; align-items: center; gap: 8px;">
        <label for="fontSize" style="font-size: 14px; margin: 0;">Taille du texte:</label>
        <input type="number" id="fontSize" min="8" max="20" value="14" style="width: 60px; padding: 5px; border: 1px solid #bdbdbd; border-radius: 4px;">
    </div>
    <button id="printButton">Imprimer le Certificat</button>
</div>
<script src="print.js"></script>
<script src="certificat-unified-font-size.js"></script>
<script>
// Sauvegarder les modifications dans le localStorage
function sauvegarderModifications() {
const polycliniqueInput = document.getElementById('polyclinique');
const polycliniqueArInput = document.getElementById('polyclinique-ar');
const docteurInput = document.getElementById('docteur');

// Sauvegarder les valeurs dans le localStorage
polycliniqueInput.addEventListener('input', function () {
localStorage.setItem('polyclinique', this.value);
});

polycliniqueArInput.addEventListener('input', function () {
localStorage.setItem('polyclinique-ar', this.value);
});

docteurInput.addEventListener('input', function () {
localStorage.setItem('docteur', this.value);
});
}
sauvegarderModifications();
</script>
</body>
</html>
`;

    const newWindow = window.open("", "_blank");
    newWindow.document.write(certificatContent);
    newWindow.document.close();
}




// Fonction pour générer un certificat de maladie chronique
function genererChronique() {
    const polyclinique = document.getElementById('polyclinique').value;
    const docteur = document.getElementById('docteur').value;
    
    // Get patient information from the form fields
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '[Nom du patient]';
    const patientAge = document.getElementById('patientAge').value;
    const patientDateNaissance = document.getElementById('patientDateNaissance').value;
    const dateCertificat = document.getElementById('dateCertificat').value || new Date().toISOString().split('T')[0];
    
    // Construire la partie de l'âge/date de naissance
    let ageInfo = '';
    if (patientAge) {
        ageInfo = 'âgé(e) de ' + patientAge;
    } else if (patientDateNaissance) {
        ageInfo = 'né(e) le ' + patientDateNaissance;
    } else {
        ageInfo = 'né(e) le [Date de naissance]';
    }
    
    // Format the date for display
    const formattedDate = new Date(dateCertificat).toLocaleDateString('fr-FR');
    
    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';
    
    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"></div>';
    }
    
    const certificatHtml = `
<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Certificat de Maladie Chronique</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            padding: 20px;
            background-color: #f9f9f9;
        }
        .certificat {
            background-color: white;
            border: 1px solid #ddd;
            padding: 20px;
            max-width: 600px;
            margin: 0 auto;
            margin-top: 60px;
            box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
        }
        h1 {
            text-align: center;
            color: #333;
            text-decoration: underline;
            font-size: 20px;
        }
        p {
            line-height: 1.5;
            color: #555;
        }
        .signature {
            text-align: right;
            margin-top: 50px;
        }
        @media print {
            body {
                background-color: white;
            }
            .certificat {
                border: none;
                box-shadow: none;
                margin-top: 0;
            }
            .print-button {
                display: none;
            }
        }
        .print-button {
            text-align: center;
            margin-top: 20px;
        }
        .print-button button {
            padding: 10px 20px;
            font-size: 16px;
            background-color: #007bff;
            color: white;
            border: none;
            border-radius: 5px;
            cursor: pointer;
        }
        .print-button button:hover {
            background-color: #0056b3;
        }
        #head {
            margin-bottom: 20px;
        }
        #head table {
            width: 100%;
            border: 0px solid #000000;
            padding: 4px;
            margin-bottom: 15px;
        }
        #head td {
            text-align: center;
        }
    </style>
</head>
<body>
    ${enteteContent}
    <div class="certificat">
        <h1>CERTIFICAT DE MALADIE CHRONIQUE</h1>
        <p>
            Je soussigné(e), Dr <strong>${docteur || '[Nom du docteur]'}</strong>, certifie avoir examiné 
            le patient <strong>${patientNomPrenom}</strong>, ${ageInfo}.
        </p>
        <p>
            À la suite de cet examen, je constate que le patient souffre d'une maladie chronique nécessitant 
            un suivi médical régulier et des soins continus.
        </p>
        <p>
            Cette affection chronique nécessite des consultations périodiques et un traitement adapté.
        </p>
        <div class="signature">
            Fait à <strong>${polyclinique || '[Nom de la polyclinique]'}</strong>, le <strong>${formattedDate}</strong><br><br><br>
            Signature et cachet<br>
            Dr <strong>${docteur || '[Nom du docteur]'}</strong>
        </div>
    </div>
    <div class="print-button">
        <button onclick="window.print()">Imprimer le certificat</button>
    </div>
</body>
</html>
    `;
    
    const newWindow = window.open();
    newWindow.document.write(certificatHtml);
    newWindow.document.close();
}



// Fonction pour générer une lettre médicale
function genererLettre() {
    const polyclinique = document.getElementById('polyclinique').value;
    const docteur = document.getElementById('docteur').value;
    
    // Get patient information from the form fields
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '[Nom du patient]';
    const patientAge = document.getElementById('patientAge').value;
    const patientDateNaissance = document.getElementById('patientDateNaissance').value;
    const dateCertificat = document.getElementById('dateCertificat').value || new Date().toISOString().split('T')[0];
    
    // Construire la partie de l'âge/date de naissance
    let ageInfo = '';
    if (patientAge) {
        ageInfo = 'âgé(e) de ' + patientAge;
    } else if (patientDateNaissance) {
        ageInfo = 'né(e) le ' + patientDateNaissance;
    } else {
        ageInfo = 'né(e) le [Date de naissance]';
    }
    
    // Format the date for display
    const formattedDate = new Date(dateCertificat).toLocaleDateString('fr-FR');
    
    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';
    
    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"></div>';
    }
    
    const certificatHtml = `
<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Lettre Médicale</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            padding: 20px;
            background-color: #f9f9f9;
        }
        .certificat {
            background-color: white;
            border: 1px solid #ddd;
            padding: 20px;
            max-width: 600px;
            margin: 0 auto;
            margin-top: 60px;
            box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
        }
        h1 {
            text-align: center;
            color: #333;
            text-decoration: underline;
            font-size: 20px;
        }
        p {
            line-height: 1.5;
            color: #555;
        }
        .signature {
            text-align: right;
            margin-top: 50px;
        }
        @media print {
            body {
                background-color: white;
            }
            .certificat {
                border: none;
                box-shadow: none;
                margin-top: 0;
            }
            .print-button {
                display: none;
            }
        }
        .print-button {
            text-align: center;
            margin-top: 20px;
        }
        .print-button button {
            padding: 10px 20px;
            font-size: 16px;
            background-color: #007bff;
            color: white;
            border: none;
            border-radius: 5px;
            cursor: pointer;
        }
        .print-button button:hover {
            background-color: #0056b3;
        }
        #head {
            margin-bottom: 20px;
        }
        #head table {
            width: 100%;
            border: 0px solid #000000;
            padding: 4px;
            margin-bottom: 15px;
        }
        #head td {
            text-align: center;
        }
    </style>
</head>
<body>
    ${enteteContent}
    <div class="certificat">
        <h1>LETTRE MEDICALE</h1>
        <p>
            Je soussigné(e), Dr <strong>${docteur || '[Nom du docteur]'}</strong>, certifie avoir examiné 
            le patient <strong>${patientNomPrenom}</strong>, ${ageInfo}.
        </p>
        <p>
            [Contenu de la lettre médicale]
        </p>
        <p>
            Cette lettre est établie à la demande du patient pour servir et valoir ce que de droit.
        </p>
        <div class="signature">
            Fait à <strong>${polyclinique || '[Nom de la polyclinique]'}</strong>, le <strong>${formattedDate}</strong><br><br><br>
            Signature et cachet<br>
            Dr <strong>${docteur || '[Nom du docteur]'}</strong>
        </div>
    </div>
    <div class="print-button">
        <button onclick="window.print()">Imprimer la lettre</button>
    </div>
</body>
</html>
    `;
    
    const newWindow = window.open();
    newWindow.document.write(certificatHtml);
    newWindow.document.close();
}

// Fonction pour générer une lettre médicale
function genererLettre() {
    // Vider les champs de recherche dans le stockage local
    localStorage.removeItem('searchInput');
    localStorage.removeItem('searchInput2');
    localStorage.removeItem('searchInput3');
    localStorage.removeItem('searchInput4');

    // Get patient information from the form fields
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '[Nom du patient]';
    const patientAge = document.getElementById('patientAge').value;
    const patientDateNaissance = document.getElementById('patientDateNaissance').value;
    const dateCertificat = document.getElementById('dateCertificat').value || new Date().toISOString().split('T')[0];
    
    // Split patient name into first and last name
    let nom = '';
    let prenom = '';
    if (patientNomPrenom) {
        const parts = patientNomPrenom.split(' ');
        nom = parts[0] || '';
        prenom = parts.slice(1).join(' ') || '';
    }
    
    // Use date of birth if available
    const dob = patientDateNaissance || '[Date de naissance]';
    
    const docteur = localStorage.getItem('docteur') || "";
    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";

    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';

    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"><\/div>';
    }

    const certificatContent = [
        '<!DOCTYPE html>',
        '<html lang="fr">',
        '<head>',
        '<meta charset="UTF-8">',
        '<meta name="viewport" content="width=device-width, initial-scale=1.0">',
        '<title>Cher confrère<\/title>',
        '<style>',
        'body {',
        'font-family: Arial, sans-serif;',
        'padding: 20px;',
        'background-color: #f9f9f9;',
        '}',
        '.certificat {',
        'background-color: white;',
        'border: 1px solid #ddd;',
        'padding: 20px;',
        'max-width: 600px;',
        'margin: 0 auto;',
        'margin-top: 60px;',
        'box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);',
        '}',
        'h1 {',
        'text-align: center;',
        'color: #333;',
        'text-decoration: underline;',
        'font-size: 20px;',
        '}',
        'p {',
        'line-height: 1.5;',
        'color: #555;',
        '}',
        '.editable-field {',
        'border-bottom: 1px dashed #666;',
        'display: inline-block;',
        'min-width: 50px;',
        'min-height: 20px;',
        'padding: 2px 4px;',
        'margin: 0 3px;',
        '}',
        '.editable-area {',
        'border: 1px solid #ddd;',
        'border-radius: 4px;',
        'padding: 8px;',
        'margin: 5px 0;',
        'width: 100%;',
        'min-height: 20px;',
        'resize: vertical;',
        'overflow: hidden;',
        'font-family: inherit;',
        'font-size: inherit;',
        'line-height: inherit;',
        '}',
        '.editable-area:focus {',
        'outline: none;',
        'border-color: #007bff;',
        '}',
        '#head {',
        'margin-bottom: 20px;',
        '}',
        '#head table {',
        'width: 100%;',
        'border: 0px solid #000000;',
        'padding: 4px;',
        'margin-bottom: 15px;',
        '}',
        '#head td {',
        'text-align: center;',
        '}',
        '.print-button {',
        'text-align: center;',
        'margin-top: 20px;',
        '}',
        '.print-button button {',
        'padding: 10px 20px;',
        'font-size: 16px;',
        'background-color: #007bff;',
        'color: white;',
        'border: none;',
        'border-radius: 5px;',
        'cursor: pointer;',
        '}',
        '.print-button button:hover {',
        'background-color: #0056b3;',
        '}',
        '@media print {',
        '@page {',
        '    size: A5;',
        '    margin: 0.2cm 0.2cm 0.2cm 0.2cm;',
        '}',
        '',
        'body {',
        '    margin: 0 !important;',
        '    padding: 0 !important;',
        '    font-size: 10pt !important;',
        '    line-height: 1.2 !important;',
        '    background-color: white;',
        '}',
        '',
        '.certificat {',
        '    border: none;',
        '    box-shadow: none;',
        '    margin: 0 !important;',
        '    padding: 2px 8px !important;',
        '    max-width: 100% !important;',
        '}',
        '',
        'h1 {',
        '    font-size: 14pt !important;',
        '    margin: 5px 0 !important;',
        '    margin-top: 2cm !important;',
        '}',
        '',
        'input[type="text"],',
        'input[type="date"],',
        'textarea {',
        '    border: none !important;',
        '    background: none !important;',
        '    box-shadow: none !important;',
        '    outline: none !important;',
        '    font-size: 9pt !important;',
        '}',
        '',
        'input[type="text"]:focus,',
        'input[type="date"]:focus,',
        'textarea:focus {',
        '    border: none !important;',
        '    outline: none !important;',
        '}',
        '',
        '/* Styles existants */',
        '.print-button {',
        '    display: none;',
        '}',
        '.editable-field, .editable-area {',
        '    border: none !important;',
        '}',
        '',
        '/* Additional space optimization */',
        '* {',
        '    margin-top: 0 !important;',
        '    margin-bottom: 2px !important;',
        '}',
        '',
        'p {',
        '    margin: 2px 0 !important;',
        '    font-size: 9pt !important;',
        '}',
        '}',
        '<\/style>',
        '<\/head>',
        '<body>',
        enteteContent,
        '',
        '<div class="certificat">',
        '<h1>Cher confrère<\/h1>',
        '<p>',
        'Permettez-moi de vous adresser le(a) sus nommé(e)',
        '<strong><input type="text" value="' + nom + ' ' + prenom + '" class="editable-input"><\/strong>,',
        'né(e) le <strong><input type="text" value="' + dob + '" class="editable-input"><\/strong>,',
        'qui consulte chez nous pour :<br>',
        '<input type="text" id="searchInput" placeholder="Raison de la consultation" class="full-width-input">',
        '',
        '<div class="optional-field">',
        '<input type="text" value="Il(elle) a comme ATCD: " class="label-input">',
        '<input type="text" id="searchInput2" placeholder="Antécédents médicaux" class="full-width-input">',
        '<\/div>',
        '',
        '<div class="optional-field">',
        '<input type="text" value="L\'examen clinique présent: " class="label-input">',
        '<input type="text" id="searchInput3" placeholder="Examen clinique" class="full-width-input">',
        '<\/div>',
        '',
        '<div class="optional-field">',
        '<input type="text" value="Qui fait évoquer: " class="label-input">',
        '<input type="text" id="searchInput4" placeholder="Diagnostic" class="full-width-input">',
        '<\/div>',
        '',
        '<p>Je vous le(la) confie pour avis et éventuelle prise en charge spécialisée.<\/p>',
        '<\/p>',
'<p style="text-align: right; margin-right: 50px;">',
            'Confraternellement Dr docteur',
            '<\/p>',
        '<\/div>',
        '',
        '<style>',
        '.editable-input {',
        'border: 1px solid #ddd;',
        'padding: 2px 5px;',
        'margin: 0 3px;',
        'min-width: 100px;',
        '}',
        '.full-width-input {',
        'width: 100%;',
        'border: 1px solid #ddd;',
        'padding: 8px;',
        'margin: 5px 0;',
        'box-sizing: border-box;',
        '}',
        '.label-input {',
        'border: none;',
        'background: none;',
        'font-weight: bold;',
        'padding: 2px 5px;',
        'margin-right: 5px;',
        '}',
        '.optional-field {',
        'margin: 10px 0;',
        '}',
        '',
        '@media print {',
        'input {',
        'border: none !important;',
        'background: none !important;',
        '}',
        '.label-input {',
        'padding: 0 !important;',
        '}',
        'input::placeholder {',
        'color: transparent; /* Rendre le placeholder transparent lors de l\'impression */',
        '}',
        '}',
        '<\/style>',
        '<div class="print-button" style="display: flex; align-items: center; justify-content: center; gap: 15px;">',
        '    <div style="display: flex; align-items: center; gap: 8px;">',
        '        <label for="fontSize" style="font-size: 14px; margin: 0;">Taille du texte:<\/label>',
        '        <input type="number" id="fontSize" min="8" max="20" value="14" style="width: 60px; padding: 5px; border: 1px solid #bdbdbd; border-radius: 4px;">',
        '    <\/div>',
        '    <button id="printButton">Imprimer la lettre<\/button>',
        '<\/div>',
        '<script>',
        '// Initialisation des champs de la lettre',
        'document.addEventListener(\'DOMContentLoaded\', function() {',
        '    // Gestion des champs de la polyclinique et du docteur',
        '    const polycliniqueInput = document.getElementById(\'polyclinique\');',
        '    const polycliniqueArInput = document.getElementById(\'polyclinique-ar\');',
        '    const docteurInput = document.querySelector(\'[contenteditable]\');',
        '',
        '    if (polycliniqueInput) {',
        '        polycliniqueInput.addEventListener(\'input\', function() {',
        '            localStorage.setItem(\'polyclinique\', this.value);',
        '        });',
        '    }',
        '',
        '    if (polycliniqueArInput) {',
        '        polycliniqueArInput.addEventListener(\'input\', function() {',
        '            localStorage.setItem(\'polyclinique-ar\', this.value);',
        '        });',
        '    }',
        '',
        '    if (docteurInput) {',
        '        docteurInput.addEventListener(\'input\', function() {',
        '            localStorage.setItem(\'docteur\', this.textContent);',
        '        });',
        '    }',
        '',
        '    // Adaptation automatique de la hauteur',
        '    const editableAreas = document.querySelectorAll(\'.editable-area\');',
        '    editableAreas.forEach(area => {',
        '        area.style.height = \'auto\';',
        '        area.style.height = (area.scrollHeight) + \'px\';',
        '',
        '        area.addEventListener(\'input\', function() {',
        '            this.style.height = \'auto\';',
        '            this.style.height = (this.scrollHeight) + \'px\';',
        '        });',
        '    });',
        '',
        '    // Sauvegarder les champs de base',
        '    const fields = [\'searchInput\', \'searchInput2\', \'searchInput3\', \'searchInput4\'];',
        '    fields.forEach(id => {',
        '        const element = document.getElementById(id);',
        '        if (element) {',
        '            // Restaurer la valeur sauvegardée',
        '            const savedValue = localStorage.getItem(id);',
        '            if (savedValue) {',
        '                element.value = savedValue;',
        '            }',
        '            // Ajouter l\'écouteur d\'événement',
        '            element.addEventListener(\'input\', function() {',
        '                localStorage.setItem(id, this.value);',
        '            });',
        '        }',
        '    });',
        '',
        '    // Ajouter l\'écouteur d\'événement pour le bouton d\'impression',
        '    const printButton = document.getElementById(\'printButton\');',
        '    if (printButton) {',
        '        printButton.addEventListener(\'click\', function() {',
        '            window.print();',
        '        });',
        '    }',
        '});',
        '<\/script>',
        '<\/body>',
        '<\/html>'
    ].join('\n');

    const newWindow = window.open("", "_blank");
    if (newWindow) {
        newWindow.document.write(certificatContent);
        newWindow.document.close();
    } else {
        console.error("Impossible d'ouvrir une nouvelle fenetre. Veuillez vérifier les paramètres de blocage des fenetres popup.");
    }
}

// Fonction pour générer un certificat Radiox
function genererRadiox() {
    // Vider uniquement les champs de consultation et type d'exploration
    localStorage.removeItem('raisonConsultation');
    localStorage.removeItem('typeExploration');

    // Get patient information from the form fields
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '[Nom du patient]';
    const patientAge = document.getElementById('patientAge').value;
    const patientDateNaissance = document.getElementById('patientDateNaissance').value;
    const dateCertificat = document.getElementById('dateCertificat').value || new Date().toISOString().split('T')[0];
    
    // Split patient name into first and last name
    let nom = '';
    let prenom = '';
    if (patientNomPrenom) {
        const parts = patientNomPrenom.split(' ');
        nom = parts[0] || '';
        prenom = parts.slice(1).join(' ') || '';
    }
    
    // Use date of birth if available
    const dob = patientDateNaissance || '[Date de naissance]';
    
    const docteur = localStorage.getItem('docteur') || "";

    const today = new Date();
    const year = today.getFullYear();
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const day = String(today.getDate()).padStart(2, '0');
    const todayFormatted = `${year}-${month}-${day}`;

    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";
    
    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';
    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"></div>';
    }

    const certificatContent = [
        '<!DOCTYPE html>',
        '<html lang="fr">',
        '<head>',
        '<meta charset="UTF-8">',
        '<meta name="viewport" content="width=device-width, initial-scale=1.0">',
        '<title>Radiox</title>',
        '<style>',
        '/* Général */',
        'body {',
        '    font-family: Arial, sans-serif;',
        '    padding: 20px;',
        '    background-color: #f9f9f9;',
        '}',
        '.certificat {',
        '    background-color: white;',
        '    border: 1px solid #ddd;',
        '    padding: 20px;',
        '    max-width: 600px;',
        '    margin: 0 auto;',
        '    margin-top: 60px;',
        '    box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);',
        '}',
        'h1 {',
        '    text-align: center;',
        '    color: #333;',
        '    text-decoration: underline;',
        '    font-size: 20px;',
        '}',
        'p {',
        '    line-height: 1.5;',
        '    color: #555;',
        '}',
        '.editable-area {',
        '    border: 1px solid #ddd;',
        '    border-radius: 4px;',
        '    padding: 8px;',
        '    margin: 5px 0;',
        '    width: 100%;',
        '    min-height: 20px;',
        '    resize: vertical;',
        '    overflow: hidden;',
        '    font-family: inherit;',
        '    font-size: inherit;',
        '    line-height: inherit;',
        '}',
        '.search-box {',
        '    width: 200px;',
        '    padding: 8px;',
        '    border: 1px solid #ddd;',
        '    border-radius: 4px;',
        '    margin-right: 10px;',
        '}',
        '.lock-button {',
        '    background: none;',
        '    border: none;',
        '    cursor: pointer;',
        '    padding: 0;',
        '    margin-left: 10px;',
        '}',
        '.lock-button:hover {',
        '    opacity: 0.8;',
        '}',
        '#suggestions {',
        '    position: absolute;',
        '    background: white;',
        '    border: 1px solid #ddd;',
        '    border-radius: 4px;',
        '    padding: 5px;',
        '    margin-top: 5px;',
        '    max-height: 200px;',
        '    overflow-y: auto;',
        '    width: 200px;',
        '    z-index: 1000;',
        '}',
        '.suggestion-item {',
        '    padding: 5px;',
        '    cursor: pointer;',
        '    border-bottom: 1px solid #eee;',
        '}',
        '.suggestion-item:hover {',
        '    background-color: #f0f0f0;',
        '}',
        '.print-button {',
        '    text-align: center;',
        '    margin-top: 20px;',
        '}',
        '.print-button button {',
        '    padding: 10px 20px;',
        '    font-size: 16px;',
        '    background-color: #007bff;',
        '    color: white;',
        '    border: none;',
        '    border-radius: 5px;',
        '    cursor: pointer;',
        '}',
        '.print-button button:hover {',
        '    background-color: #0056b3;',
        '}',
        '@media print {',
        '    @page {',
        '        size: A5;',
        '        margin: 0.2cm 0.2cm 0.2cm 0.2cm;',
        '    }',
        '',
        '    body {',
        '        margin: 0 !important;',
        '        padding: 0 !important;',
        '        font-size: 10pt !important;',
        '        line-height: 1.2 !important;',
        '        background-color: white;',
        '}',
        '',
        '.certificat {',
        '    padding: 2px 8px !important;',
        '    max-width: 100% !important;',
        '    border: none;',
        '    box-shadow: none;',
        '    margin-top: 0;',
        '}',
        '',
        'h1 {',
        '    font-size: 14pt !important;',
        '    margin: 5px 0 !important;',
        '    margin-top: 2cm !important;',
        '}',
        '',
        'p {',
        '    font-size: 9pt !important;',
        '    margin: 2px 0 !important;',
        '    line-height: 1.2 !important;',
        '}',
        '',
        'input[type="text"],',
        'input[type="date"],',
        'textarea {',
        '    border: none !important;',
        '    background: none !important;',
        '    box-shadow: none !important;',
        '    outline: none !important;',
        '    font-size: 9pt !important;',
        '}',
        '',
        'input[type="text"]:focus,',
        'input[type="date"]:focus,',
        'textarea:focus {',
        '    border: none !important;',
        '    outline: none !important;',
        '}',
        '',
        '/* Rendre le placeholder transparent lors de l\'impression */',
        'input::placeholder,',
        'textarea::placeholder {',
        '    color: transparent;',
        '}',
        '',
        '.print-button {',
        '    display: none;',
        '}',
        '',
        '.editable-area {',
        '    border: none !important;',
        '}',
        '',
        '/* Additional space optimization */',
        '* {',
        '    margin-top: 0 !important;',
        '    margin-bottom: 2px !important;',
        '}',
        '',
        'p {',
        '    margin: 2px 0 !important;',
        '    font-size: 9pt !important;',
        '}',
        '}',
        '<\/style>',
        '<\/head>',
        '<body>',
        enteteContent,
        '<div class="certificat">',
        '<h1>Cher confrère<\/h1>',
'<div class="contenu-certificat" style="margin-top: 1.5cm !important;">',
        '<p>',
'Permettez-moi de vous adresser le(a) nommé(e) <strong><input type="text" value="' + nom + ' ' + prenom + '" style="width: 180px;" id="patientNomPrenom"><\/strong>,',
        'né(e) le <strong><input type="text" value="' + dob + '" style="width: 120px;" id="patientDateNaissance"><\/strong>, qui consulte chez nous pour :<br>',
        '<textarea id="raisonConsultation" class="editable-area" placeholder="Raison de la consultation"><\/textarea>',
        '',
        'Pour faire un :<br>',
        '<textarea id="typeExploration" class="editable-area" placeholder="Type d\'exploration"><\/textarea>',
'<\/p>',
        '<p style="text-align: right; margin-top: 30px;">',
        'Confraternellement<br>',
        '<span class="docteur" style="font-weight: bold;">Dr ' + docteur + '<\/span>',
        '<\/p>',
		
'<\/div>',
        '<\/div>',
        '<div class="print-button" style="display: flex; align-items: center; justify-content: center; gap: 15px;">',
        '    <div style="display: flex; align-items: center; gap: 8px;">',
        '        <label for="fontSize" style="font-size: 14px; margin: 0;">Taille du texte:<\/label>',
        '        <input type="number" id="fontSize" min="8" max="20" value="14" style="width: 60px; padding: 5px; border: 1px solid #bdbdbd; border-radius: 4px;">',
        '    <\/div>',
        '    <button id="printButton">Imprimer la lettre<\/button>',
        '<\/div>',
        '<script src="print.js"><\/script>',
        '<script src="certificat-unified-font-size.js"><\/script>',
        '<script>',
        '// Sauvegarder les modifications dans le localStorage',
        'function sauvegarderModifications() {',
        'const polycliniqueInput = document.getElementById(\'polyclinique\');',
        'const polycliniqueArInput = document.getElementById(\'polyclinique-ar\');',
        'const docteurInput = document.getElementById(\'docteur\');',
        '',
        '// Sauvegarder les valeurs dans le localStorage',
        'polycliniqueInput.addEventListener(\'input\', function () {',
        'localStorage.setItem(\'polyclinique\', this.value);',
        '});',
        '',
        'polycliniqueArInput.addEventListener(\'input\', function () {',
        'localStorage.setItem(\'polyclinique-ar\', this.value);',
        '});',
        '',
        'docteurInput.addEventListener(\'input\', function () {',
        'localStorage.setItem(\'docteur\', this.value);',
        '});',
        '',
        '// Sauvegarder les champs de base',
        'const fields = [\'docteur\'];',
        'fields.forEach(id => {',
        '    const element = document.getElementById(id);',
        '    if (element) {',
        '        // Restaurer la valeur sauvegardée',
        '        const savedValue = localStorage.getItem(id);',
        '        if (savedValue) {',
        '            element.value = savedValue;',
        '        }',
        '        // Ajouter l\'écouteur d\'événement',
        '        element.addEventListener(\'input\', function() {',
        '            localStorage.setItem(id, this.value);',
        '        });',
        '    }',
        '});',
        '',
        '// Ajouter l\'écouteur d\'événement pour le bouton d\'impression',
        'document.addEventListener(\'DOMContentLoaded\', function() {',
        '    const printButton = document.getElementById(\'printButton\');',
        '    if (printButton) {',
        '        printButton.addEventListener(\'click\', function() {',
        '            window.print();',
        '        });',
        '    }',
        '});',
        '}',
        'sauvegarderModifications();',
        '<\/script>',
        '<\/body>',
        '<\/html>'
    ].join('\n');

    const newWindow = window.open("", "_blank");
    if (newWindow) {
        newWindow.document.write(certificatContent);
        newWindow.document.close();
    } else {
        console.error("Impossible d'ouvrir une nouvelle fenetre. Veuillez vérifier les paramètres de blocage des fenetres popup.");
    }
}

// Fonction pour générer un certificat de reprise de travail
function genererReprise() {
    // Get patient information from the form fields
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '[Nom du patient]';
    const patientAge = document.getElementById('patientAge').value;
    const patientDateNaissance = document.getElementById('patientDateNaissance').value;
    const dateCertificat = document.getElementById('dateCertificat').value || new Date().toISOString().split('T')[0];
    
    // Split patient name into first and last name
    let nom = '';
    let prenom = '';
    if (patientNomPrenom) {
        const parts = patientNomPrenom.split(' ');
        nom = parts[0] || '';
        prenom = parts.slice(1).join(' ') || '';
    }
    
    // Use date of birth if available
    const dob = patientDateNaissance || '[Date de naissance]';
    
    const docteur = localStorage.getItem('docteur') || "";
    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";

    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';

    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"><\/div>';
    }

    const today = new Date();
    const year = today.getFullYear();
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const day = String(today.getDate()).padStart(2, '0');
    const todayFormatted = `${day}/${month}/${year}`;

    const certificatContent = [
        '<!DOCTYPE html>',
        '<html lang="fr">',
        '<head>',
        '<meta charset="UTF-8">',
        '<meta name="viewport" content="width=device-width, initial-scale=1.0">',
        '<title>Reprise de travail<\/title>',
        '<style>',
        'body {',
        '    font-family: Arial, sans-serif;',
        '    padding: 20px;',
        '    background-color: #f9f9f9;',
        '}',
        '.certificat {',
        '    background-color: white;',
        '    border: 1px solid #ddd;',
        '    padding: 20px;',
        '    max-width: 600px;',
        '    margin: 0 auto;',
        '    margin-top: 60px;',
        '    box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);',
        '}',
        'h1 {',
        '    text-align: center;',
        '    color: #333;',
        '    text-decoration: underline;',
        '    font-size: 20px;',
        '}',
        'p {',
        '    line-height: 1.5;',
        '    color: #555;',
        '}',
        '.editable-field {',
        '    border-bottom: 1px dashed #666;',
        '    display: inline-block;',
        '    min-width: 50px;',
        '    min-height: 20px;',
        '    padding: 2px 4px;',
        '    margin: 0 3px;',
        '}',
        '.editable-area {',
        '    border: 1px solid #ddd;',
        '    border-radius: 4px;',
        '    padding: 8px;',
        '    margin: 5px 0;',
        '    width: 100%;',
        '    min-height: 20px;',
        '    resize: vertical;',
        '    overflow: hidden;',
        '    font-family: inherit;',
        '    font-size: inherit;',
        '    line-height: inherit;',
        '}',
        '.editable-area:focus {',
        '    outline: none;',
        '    border-color: #007bff;',
        '}',
        '#head {',
        '    margin-bottom: 20px;',
        '}',
        '#head table {',
        '    width: 100%;',
        '    border: 0px solid #000000;',
        '    padding: 4px;',
        '    margin-bottom: 15px;',
        '}',
        '#head td {',
        '    text-align: center;',
        '}',
        '.print-button {',
        '    text-align: center;',
        '    margin-top: 20px;',
        '}',
        '.print-button button {',
        '    padding: 10px 20px;',
        '    font-size: 16px;',
        '    background-color: #007bff;',
        '    color: white;',
        '    border: none;',
        '    border-radius: 5px;',
        '    cursor: pointer;',
        '}',
        '.print-button button:hover {',
        '    background-color: #0056b3;',
        '}',
        '@media print {',
        '    @page {',
        '        size: A5;',
        '        margin: 0.2cm 0.2cm 0.2cm 0.2cm;',
        '    }',
        '    body {',
        '        margin: 0 !important;',
        '        padding: 0 !important;',
        '        font-size: 10pt !important;',
        '        line-height: 1.2 !important;',
        '        background-color: white;',
        '    }',
        '    .certificat {',
        '        border: none;',
        '        box-shadow: none;',
        '        margin: 0 !important;',
        '        padding: 2px 8px !important;',
        '        max-width: 100% !important;',
        '    }',
        '    h1 {',
        '        font-size: 14pt !important;',
        '        margin: 5px 0 !important;',
        '        margin-top: 2cm !important;',
        '    }',
        '    p {',
        '        font-size: 9pt !important;',
        '        margin: 2px 0 !important;',
        '        line-height: 1.2 !important;',
        '    }',
        '    input[type="text"],',
        '    input[type="date"],',
        '    textarea {',
        '        border: none !important;',
        '        background: none !important;',
        '        box-shadow: none !important;',
        '        outline: none !important;',
        '        font-size: 9pt !important;',
        '    }',
        '    input[type="text"]:focus,',
        '    input[type="date"]:focus,',
        '    textarea:focus {',
        '        border: none !important;',
        '        outline: none !important;',
        '    }',
        '    input::placeholder,',
        '    textarea::placeholder {',
        '        color: transparent;',
        '    }',
        '    .print-button {',
        '        display: none;',
        '    }',
        '    .editable-field, .editable-area {',
        '        border: none !important;',
        '    }',
        '    /* Additional space optimization */',
        '    * {',
        '        margin-top: 0 !important;',
        '        margin-bottom: 2px !important;',
        '    }',
        '}',
        '<\/style>',
        '<\/head>',
        '<body>',
        enteteContent,
        '<div class="certificat">',
        '<h1>Certificat médical de reprise de travail<\/h1>',
        '<br><br><br>',
        '<p>',
        'Je soussigné, Dr <input type="text" id="docteur" value="' + docteur + '" placeholder="" style="min-width: 150px;">, certifie avoir examiné ce jour',
        'le(a) nommé(e) <strong><input type="text" value="' + nom + ' ' + prenom + '" style="width: 180px;"><\/strong>,',
        '<span class="editable-field" contenteditable="true" style="min-width: 100px; display: inline-block;">né(e) le ' + dob + '<\/span>.<br>',
        'Déclare que son état de santé lui permet de reprendre son travail le : <span class="editable-field" contenteditable="true" style="min-width: 120px; display: inline-block;">' + todayFormatted + '<\/span><br>',
        'Sauf complication.<br>',
        '<\/p>',
        '<p style="text-align: right;">DONT CERTIFICAT<br>',
        '<span class="docteur" style="font-weight: bold;">Dr ' + docteur + '<\/span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;',
        '<\/p>',
        '<\/div>',
        '<div class="print-button" style="display: flex; align-items: center; justify-content: center; gap: 15px;">',
        '    <div style="display: flex; align-items: center; gap: 8px;">',
        '        <label for="fontSize" style="font-size: 14px; margin: 0;">Taille du texte:<\/label>',
        '        <input type="number" id="fontSize" min="8" max="20" value="14" style="width: 60px; padding: 5px; border: 1px solid #bdbdbd; border-radius: 4px;">',
        '    <\/div>',
        '    <button id="printButton">Imprimer le Certificat<\/button>',
        '<\/div>',
        '<script>',
        '// Sauvegarder les modifications dans le localStorage',
        'function sauvegarderModifications() {',
        '    const docteurInput = document.getElementById(\'docteur\');',
        '',
        '    // Sauvegarder les valeurs dans le localStorage',
        '    if (docteurInput) {',
        '        docteurInput.addEventListener(\'input\', function () {',
        '            localStorage.setItem(\'docteur\', this.value);',
        '        });',
        '    }',
        '',
        '    // Sauvegarder les champs de base',
        '    const fields = [\'docteur\'];',
        '    fields.forEach(id => {',
        '        const element = document.getElementById(id);',
        '        if (element) {',
        '            // Restaurer la valeur sauvegardée',
        '            const savedValue = localStorage.getItem(id);',
        '            if (savedValue) {',
        '                element.value = savedValue;',
        '            }',
        '            // Ajouter l\'écouteur d\'événement',
        '            element.addEventListener(\'input\', function() {',
        '                localStorage.setItem(id, this.value);',
        '            });',
        '        }',
        '    });',
        '}',
        '',
        '// Appeler la fonction de sauvegarde',
        'sauvegarderModifications();',
        '',
        '// Ajouter l\'écouteur d\'événement pour le bouton d\'impression',
        'document.addEventListener(\'DOMContentLoaded\', function() {',
        '    const printButton = document.getElementById(\'printButton\');',
        '    if (printButton) {',
        '        printButton.addEventListener(\'click\', function() {',
        '            window.print();',
        '        });',
        '    }',
        '});',
        '<\/script>',
        '<\/body>',
        '<\/html>'
    ].join('\n');

    const newWindow = window.open("", "_blank");
    if (newWindow) {
        newWindow.document.write(certificatContent);
        newWindow.document.close();
    } else {
        console.error("Impossible d'ouvrir une nouvelle fenetre. Veuillez vérifier les paramètres de blocage des fenetres popup.");
    }
}

// Fonction pour générer un certificat de non-grossesse
function genererNonGrossesse() {
    // Get patient information from the form fields
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '[Nom du patient]';
    const patientAge = document.getElementById('patientAge').value;
    const patientDateNaissance = document.getElementById('patientDateNaissance').value;
    const dateCertificat = document.getElementById('dateCertificat').value || new Date().toISOString().split('T')[0];
    
    // Split patient name into first and last name
    let nom = '';
    let prenom = '';
    if (patientNomPrenom) {
        const parts = patientNomPrenom.split(' ');
        nom = parts[0] || '';
        prenom = parts.slice(1).join(' ') || '';
    }
    
    // Use date of birth if available
    const dob = patientDateNaissance || '[Date de naissance]';
    
    const docteur = localStorage.getItem('docteur') || "";
    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";

    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';

    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"><\/div>';
    }

    const certificatContent = [
        '<!DOCTYPE html>',
        '<html lang="fr">',
        '<head>',
        '<meta charset="UTF-8">',
        '<meta name="viewport" content="width=device-width, initial-scale=1.0">',
        '<title>Certificat de Non-Grossesse<\/title>',
        '<style>',
        'body {',
        '    font-family: Arial, sans-serif;',
        '    padding: 20px;',
        '    background-color: #f9f9f9;',
        '}',
        '.certificat {',
        '    background-color: white;',
        '    border: 1px solid #ddd;',
        '    padding: 20px;',
        '    max-width: 600px;',
        '    margin: 0 auto;',
        '    margin-top: 60px;',
        '    box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);',
        '}',
        'h1 {',
        '    text-align: center;',
        '    color: #333;',
        '    text-decoration: underline;',
        '    font-size: 20px;',
        '}',
        'h2 {',
        '    text-align: center;',
        '    color: #555;',
        '    font-size: 16px;',
        '    margin-top: 5px;',
        '    margin-bottom: 15px;',
        '}',
        'p {',
        '    line-height: 1.5;',
        '    color: #555;',
        '}',
        '.editable-field {',
        '    border-bottom: 1px dashed #666;',
        '    display: inline-block;',
        '    min-width: 50px;',
        '    min-height: 20px;',
        '    padding: 2px 4px;',
        '    margin: 0 3px;',
        '}',
        '.editable-area {',
        '    border: 1px solid #ddd;',
        '    border-radius: 4px;',
        '    padding: 8px;',
        '    margin: 5px 0;',
        '    width: 100%;',
        '    min-height: 20px;',
        '    resize: vertical;',
        '    overflow: hidden;',
        '    font-family: inherit;',
        '    font-size: inherit;',
        '    line-height: inherit;',
        '}',
        '.editable-area:focus {',
        '    outline: none;',
        '    border-color: #007bff;',
        '}',
        '#head {',
        '    margin-bottom: 20px;',
        '}',
        '#head table {',
        '    width: 100%;',
        '    border: 0px solid #000000;',
        '    padding: 4px;',
        '    margin-bottom: 15px;',
        '}',
        '#head td {',
        '    text-align: center;',
        '}',
        '.print-button {',
        '    text-align: center;',
        '    margin-top: 20px;',
        '}',
        '.print-button button {',
        '    padding: 10px 20px;',
        '    font-size: 16px;',
        '    background-color: #007bff;',
        '    color: white;',
        '    border: none;',
        '    border-radius: 5px;',
        '    cursor: pointer;',
        '}',
        '.print-button button:hover {',
        '    background-color: #0056b3;',
        '}',
        '@media print {',
        '    @page {',
        '        size: A5;',
        '        margin: 0.2cm 0.2cm 0.2cm 0.2cm;',
        '    }',
        '    body {',
        '        margin: 0 !important;',
        '        padding: 0 !important;',
        '        font-size: 10pt !important;',
        '        line-height: 1.2 !important;',
        '        background-color: white;',
        '    }',
        '    .certificat {',
        '        border: none;',
        '        box-shadow: none;',
        '        margin: 0 !important;',
        '        padding: 2px 8px !important;',
        '        max-width: 100% !important;',
        '    }',
        '    h1 {',
        '        font-size: 14pt !important;',
        '        margin: 5px 0 !important;',
        '        margin-top: 2cm !important;',
        '    }',
        '    h2 {',
        '        font-size: 12pt !important;',
        '        margin: 3px 0 !important;',
        '    }',
        '    input[type="text"],',
        '    input[type="date"],',
        '    textarea {',
        '        border: none !important;',
        '        background: none !important;',
        '        box-shadow: none !important;',
        '        outline: none !important;',
        '        font-size: 9pt !important;',
        '    }',
        '    input[type="text"]:focus,',
        '    input[type="date"]:focus,',
        '    textarea:focus {',
        '        border: none !important;',
        '        outline: none !important;',
        '    }',
        '    input::placeholder,',
        '    textarea::placeholder {',
        '        color: transparent;',
        '    }',
        '    .print-button {',
        '        display: none;',
        '    }',
        '    .editable-field, .editable-area {',
        '        border: none !important;',
        '    }',
        '    /* Additional space optimization */',
        '    * {',
        '        margin-top: 0 !important;',
        '        margin-bottom: 2px !important;',
        '    }',
        '    p {',
        '        margin: 2px 0 !important;',
        '        font-size: 9pt !important;',
        '    }',
        '}',
        '<\/style>',
        '<\/head>',
        '<body>',
        enteteContent,
        '<div class="certificat">',
        '<h1>CERTIFICAT MEDICAL DE NON-GROSSESSE<\/h1>',
        '<div class="contenu-certificat" style="margin-top: 1cm !important;">',
        '<p>',
        'Je soussigné(e), Dr <input type="text" id="docteur" value="' + docteur + '" placeholder="Medecin">, certifie avoir examiné ce jour :<br>',
        '<strong><input type="text" value="' + nom + ' ' + prenom + '" style="width: 180px;"><\/strong>',
        '<br>',
        'née le : <strong><input type="text" value="' + dob + '" style="width: 120px;"><\/strong>',
        '<br>',
        'Je n\'ai constaté aucun signe clinique évocateur d\'une grossesse en cours à  la date du présent certificat.<br>',
        'Ce certificat est délivré à  la demande de l\'intéressée et remis en main propre pour servir et valoir ce que de droit.<br>',
        '<\/p>',
        '<p style="text-align: right;">Signature :<br>',
        '<span class="docteur" style="font-weight: bold;">Dr ' + docteur + '<\/span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;',
        '<\/p>',
        '<\/div>',
        '<\/div>',
        '<div class="print-button" style="display: flex; align-items: center; justify-content: center; gap: 15px;">',
        '    <div style="display: flex; align-items: center; gap: 8px;">',
        '        <label for="fontSize" style="font-size: 14px; margin: 0;">Taille du texte:<\/label>',
        '        <input type="number" id="fontSize" min="8" max="20" value="14" style="width: 60px; padding: 5px; border: 1px solid #bdbdbd; border-radius: 4px;">',
        '    <\/div>',
        '    <button id="printButton">Imprimer le Certificat<\/button>',
        '<\/div>',
        '<script>',
        '// Sauvegarder les modifications dans le localStorage',
        'function sauvegarderModifications() {',
        '    const docteurInput = document.getElementById(\'docteur\');',
        '',
        '    // Sauvegarder les valeurs dans le localStorage',
        '    if (docteurInput) {',
        '        docteurInput.addEventListener(\'input\', function () {',
        '            localStorage.setItem(\'docteur\', this.value);',
        '        });',
        '    }',
        '',
        '    // Sauvegarder les champs de base',
        '    const fields = [\'docteur\'];',
        '    fields.forEach(id => {',
        '        const element = document.getElementById(id);',
        '        if (element) {',
        '            // Restaurer la valeur sauvegardée',
        '            const savedValue = localStorage.getItem(id);',
        '            if (savedValue) {',
        '                element.value = savedValue;',
        '            }',
        '            // Ajouter l\'écouteur d\'événement',
        '            element.addEventListener(\'input\', function() {',
        '                localStorage.setItem(id, this.value);',
        '            });',
        '        }',
        '    });',
        '}',
        '',
        '// Appeler la fonction de sauvegarde',
        'sauvegarderModifications();',
        '',
        '// Ajouter l\'écouteur d\'événement pour le bouton d\'impression',
        'document.addEventListener(\'DOMContentLoaded\', function() {',
        '    const printButton = document.getElementById(\'printButton\');',
        '    if (printButton) {',
        '        printButton.addEventListener(\'click\', function() {',
        '            window.print();',
        '        });',
        '    }',
        '});',
        '<\/script>',
        '<\/body>',
        '<\/html>'
    ].join('\n');

    const newWindow = window.open("", "_blank");
    if (newWindow) {
        newWindow.document.write(certificatContent);
        newWindow.document.close();
    } else {
        console.error("Impossible d'ouvrir une nouvelle fenetre. Veuillez vérifier les paramètres de blocage des fenetres popup.");
    }
}

// Fonction pour générer un certificat de maladie chronique
function genererChronique() {
    // Get patient information from the form fields
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '[Nom du patient]';
    const patientAge = document.getElementById('patientAge').value;
    const patientDateNaissance = document.getElementById('patientDateNaissance').value;
    const dateCertificat = document.getElementById('dateCertificat').value || new Date().toISOString().split('T')[0];
    
    // Split patient name into first and last name
    let nom = '';
    let prenom = '';
    if (patientNomPrenom) {
        const parts = patientNomPrenom.split(' ');
        nom = parts[0] || '';
        prenom = parts.slice(1).join(' ') || '';
    }
    
    // Use date of birth if available
    const dob = patientDateNaissance || '[Date de naissance]';
    
    const docteur = localStorage.getItem('docteur') || "";
    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";

    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';

    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"><\/div>';
    }

    const certificatContent = [
        '<!DOCTYPE html>',
        '<html lang="fr">',
        '<head>',
        '<meta charset="UTF-8">',
        '<meta name="viewport" content="width=device-width, initial-scale=1.0">',
        '<title>Chronique<\/title>',
        '<style>',
        'body {',
        'font-family: Arial, sans-serif;',
        'padding: 20px;',
        'background-color: #f9f9f9;',
        '}',
        '.certificat {',
        'background-color: white;',
        'border: 1px solid #ddd;',
        'padding: 20px;',
        'max-width: 600px;',
        'margin: 0 auto;',
        'margin-top: 60px;',
        'box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);',
        '}',
        'h1 {',
        'text-align: center;',
        'color: #333;',
        'text-decoration: underline;',
        'font-size: 20px;',
        '}',

        'p {',
        'line-height: 1.5;',
        'color: #555;',
        '}',
        '.editable-field {',
        'border-bottom: 1px dashed #666;',
        'display: inline-block;',
        'min-width: 50px;',
        'min-height: 20px;',
        'padding: 2px 4px;',
        'margin: 0 3px;',
        '}',
        '.editable-area {',
        'border: 1px solid #ddd;',
        'border-radius: 4px;',
        'padding: 8px;',
        'margin: 5px 0;',
        'width: 100%;',
        'min-height: 20px;',
        'resize: vertical;',
        'overflow: hidden;',
        'font-family: inherit;',
        'font-size: inherit;',
        'line-height: inherit;',
        '}',
        '.editable-area:focus {',
        'outline: none;',
        'border-color: #007bff;',
        '}',
        '#head {',
        'margin-bottom: 20px;',
        '}',
        '#head table {',
        'width: 100%;',
        'border: 0px solid #000000;',
        'padding: 4px;',
        'margin-bottom: 15px;',
        '}',
        '#head td {',
        'text-align: center;',
        '}',
        '.print-button {',
        'text-align: center;',
        'margin-top: 20px;',
        '}',
        '.print-button button {',
        'padding: 10px 20px;',
        'font-size: 16px;',
        'background-color: #007bff;',
        'color: white;',
        'border: none;',
        'border-radius: 5px;',
        'cursor: pointer;',
        '}',
        '.print-button button:hover {',
        'background-color: #0056b3;',
        '}',
        '@media print {',
        '@page {',
        'size: A5;',
        'margin: 0.2cm 0.2cm 0.2cm 0.2cm;',
        '}',
        'body {',
        'margin: 0 !important;',
        'padding: 0 !important;',
        'font-size: 10pt !important;',
        'background-color: white;',
        '}',
        '.certificat {',
        'padding: 2px 8px !important;',
        'max-width: 100% !important;',
        'border: none;',
        'box-shadow: none;',
        'margin-top: 0;',
        '}',
        'h1 {',
        'font-size: 14pt !important;',
        'margin: 5px 0 !important;',
        'margin-top: 2cm !important;',
        '}',
        'p {',
        'font-size: 9pt !important;',
        'margin: 2px 0 !important;',
        'line-height: 1.2 !important;',
        '}',
        'input[type="text"],',
        'input[type="date"],',
        'textarea {',
        'border: none !important;',
        'background: none !important;',
        'box-shadow: none !important;',
        'outline: none !important;',
        'font-size: 9pt !important;',
        '}',
        'input[type="text"]:focus,',
        'input[type="date"]:focus,',
        'textarea:focus {',
        'border: none !important;',
        'outline: none !important;',
        '}',
        '/* Rendre le placeholder transparent lors de l\'impression */',
        'input::placeholder,',
        'textarea::placeholder {',
        'color: transparent;',
        '}',
        '.print-button {',
        'display: none;',
        '}',
        '.editable-field, .editable-area {',
        'border: none !important;',
        '}',
        '}',
        '<\/style>',
        '<\/head>',
        '<body>',
        enteteContent,
        '<div class="certificat">',
        '<h1>Certificat médical de maladie chronique<\/h1>',
        '<br>',
        '<p>',
        'Je soussigné, Dr <input type="text" id="docteur" value="' + docteur + '" placeholder="Medecin">, certifie que',
        'le(a) nommé(e) <strong><input type="text" value="' + nom + ' ' + prenom + '" style="width: 180px;"><\/strong>,',
        '<span class="editable-field" contenteditable="true" style="min-width: 100px; display: inline-block;">né(e) le ' + dob + '<\/span>.<br>',
        'Présente une maladie chronique de type :<br>',
        '<textarea placeholder="Type de maladie chronique" style="width: 100%;"><\/textarea><br>',
        'Depuis :<br>',
        '<input type="text" placeholder="Date de début"><br>',
        'Nécessitant un traitement à  long cours à  base de : <br>',
        '<textarea placeholder="Traitement" style="width: 100%;"><\/textarea><br>',
        'Ce certificat est établi sur les renseignements fournis par le(a) patient(e) et délivré à  la demande de l\'intéressé(e) pour servir et valoir ce que de droit.<br>',
        '<\/p>',
        '<p style="text-align: right;">DONT CERTIFICAT<br>',
        '<span class="docteur" style="font-weight: bold;">Dr ' + docteur + '<\/span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;',
        '<\/p>',
        '<\/div>',
        '<div class="print-button" style="display: flex; align-items: center; justify-content: center; gap: 15px;">',
        '    <div style="display: flex; align-items: center; gap: 8px;">',
        '        <label for="fontSize" style="font-size: 14px; margin: 0;">Taille du texte:<\/label>',
        '        <input type="number" id="fontSize" min="8" max="20" value="14" style="width: 60px; padding: 5px; border: 1px solid #bdbdbd; border-radius: 4px;">',
        '    <\/div>',
        '    <button id="printButton">Imprimer le Certificat<\/button>',
        '<\/div>',
        '<script>',
        '// Sauvegarder les modifications dans le localStorage',
        'function sauvegarderModifications() {',
        'const polycliniqueInput = document.getElementById(\'polyclinique\');',
        'const polycliniqueArInput = document.getElementById(\'polyclinique-ar\');',
        'const docteurInput = document.getElementById(\'docteur\');',
        '',
        '// Sauvegarder les valeurs dans le localStorage',
        'if (polycliniqueInput) {',
        '    polycliniqueInput.addEventListener(\'input\', function () {',
        '        localStorage.setItem(\'polyclinique\', this.value);',
        '    });',
        '}',
        '',
        'if (polycliniqueArInput) {',
        '    polycliniqueArInput.addEventListener(\'input\', function () {',
        '        localStorage.setItem(\'polyclinique-ar\', this.value);',
        '    });',
        '}',
        '',
        'if (docteurInput) {',
        '    docteurInput.addEventListener(\'input\', function () {',
        '        localStorage.setItem(\'docteur\', this.value);',
        '    });',
        '}',
        '',
        '// Sauvegarder les champs de base',
        'const fields = [\'docteur\'];',
        'fields.forEach(id => {',
        '    const element = document.getElementById(id);',
        '    if (element) {',
        '        // Restaurer la valeur sauvegardée',
        '        const savedValue = localStorage.getItem(id);',
        '        if (savedValue) {',
        '            element.value = savedValue;',
        '        }',
        '        // Ajouter l\'écouteur d\'événement',
        '        element.addEventListener(\'input\', function() {',
        '            localStorage.setItem(id, this.value);',
        '        });',
        '    }',
        '});',
        '}',
        '',
        '// Appeler la fonction de sauvegarde',
        'sauvegarderModifications();',
        '',
        '// Ajouter l\'écouteur d\'événement pour le bouton d\'impression',
        'document.addEventListener(\'DOMContentLoaded\', function() {',
        '    const printButton = document.getElementById(\'printButton\');',
        '    if (printButton) {',
        '        printButton.addEventListener(\'click\', function() {',
        '            window.print();',
        '        });',
        '    }',
        '});',
        '<\/script>',
        '<\/body>',
        '<\/html>'
    ].join('\n');

    const newWindow = window.open("", "_blank");
    if (newWindow) {
        newWindow.document.write(certificatContent);
        newWindow.document.close();
    } else {
        console.error("Impossible d'ouvrir une nouvelle fenetre. Veuillez vérifier les paramètres de blocage des fenetres popup.");
    }
}

// Fonction pour générer un certificat CBV
function genererCvb() {
    // Get patient information from the form fields
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '[Nom du patient]';
    const patientAge = document.getElementById('patientAge').value;
    const patientDateNaissance = document.getElementById('patientDateNaissance').value;
    const dateCertificat = document.getElementById('dateCertificat').value || new Date().toISOString().split('T')[0];


    const today = new Date();
    const year = today.getFullYear();
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const day = String(today.getDate()).padStart(2, '0');
    const todayFormatted = `${year}-${month}-${day}`;

    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";
    const docteur = localStorage.getItem('docteur') || "";

    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';

    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"></div>';
    }


    const certificatContent = `
<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CBV</title>
    <style>
body {
font-family: Arial, sans-serif;
padding: 20px;
background-color: #f9f9f9;
}
.certificat {
background-color: white;
border: 1px solid #ddd;
padding: 20px;
max-width: 600px;
margin: 0 auto;
margin-top: 60px;
box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
}
h1 {
text-align: center;
color: #333;
text-decoration: underline;
font-size: 20px;
}
p {
line-height: 1.5;
color: #555;
}
.editable-field {
border-bottom: 1px dashed #666;
display: inline-block;
min-width: 50px;
min-height: 20px;
padding: 2px 4px;
margin: 0 3px;
}
.editable-area {
border: 1px solid #ddd;
border-radius: 4px;
padding: 8px;
margin: 5px 0;
width: 100%;
min-height: 20px;
resize: vertical;
overflow: hidden;
font-family: inherit;
font-size: inherit;
line-height: inherit;
}
.editable-area:focus {
outline: none;
border-color: #007bff;
}
#head {
margin-bottom: 20px;
}
#head table {
width: 100%;
border: 0px solid #000000;
padding: 4px;
margin-bottom: 15px;
}
#head td {
text-align: center;
}
.print-button {
text-align: center;
margin-top: 20px;
}
.print-button button {
padding: 10px 20px;
font-size: 16px;
background-color: #007bff;
color: white;
border: none;
border-radius: 5px;
cursor: pointer;
}
.print-button button:hover {
background-color: #0056b3;
}
@media print {
@page {
size: A5;
margin: 0.2cm 0.2cm 0.2cm 0.2cm;
}
body {
margin: 0 !important;
padding: 0 !important;
font-size: 10pt !important;
background-color: white;
}
.certificat {
padding: 2px 8px !important;
max-width: 100% !important;
border: none;
box-shadow: none;
margin-top: 0;
}
h1 {
font-size: 14pt !important;
margin: 5px 0 !important;
margin-top: 2cm !important;
}
p {
font-size: 9pt !important;
margin: 2px 0 !important;
line-height: 1.2 !important;
}
input[type="text"],
input[type="date"],
input[type="time"],
select,
textarea {
border: none !important;
background: none !important;
box-shadow: none !important;
outline: none !important;
font-size: 9pt !important;
}
input[type="text"]:focus,
input[type="date"]:focus,
input[type="time"]:focus,
select:focus,
textarea:focus {
border: none !important;
outline: none !important;
}
.print-button {
display: none;
}

/* Masquer les contrôles d'impression et de sauvegarde */
.print-button div[style*="align-items: center"],
.print-button button {
    display: none !important;
}

.editable-field, .editable-area {
border: none !important;
}
}
</style>
</head>
<body>
${enteteContent}
    <div class="certificat">
        <h1>Certificat médical descriptif</h1>
        <p>
            Je soussigné, Dr <input type="text" id="docteur" value="${docteur}" placeholder="" style="width: 120px;">, 
            certifie avoir examiné ce jour le(la) susnommé(e) 
            <strong><input type="text" value="${patientNomPrenom}" style="width: 180px;"></strong>,
            <span class="editable-field" contenteditable="true" style="min-width: 100px; display: inline-block;">né(e) le ${patientDateNaissance}</span>.
        </p>
        <p>
            qui m'a déclaré avoir été victime de 	<select id="typeAccident" style="
    width: 160px;
    padding: 5px;
    margin: 5px 0;
    border: none;
    background-color: transparent;
    appearance: none;
    -webkit-appearance: none;
    -moz-appearance: none;
"><option value="cbv">CBV</option>
        <option value="accident_at">Accident de travail</option>
        <option value="accident_circulation">Accident de circulation</option>
        <option value="accident_sportif">Accident sportif</option>
        <option value="autre">Autre</option>
    </select>
</strong>,
             le <span class="editable-field" contenteditable="true" style="min-width: 120px; display: inline-block;">${todayFormatted}</span> à  l'heure:<input type="time" id="heureAccident" style="font-size: 11px !important;"> <br> 
            L'examen clinique présente :<br> 
		

        <br>
    <input type="text" id="descriptionAccident" placeholder="" style="width: 180px; margin: 5px 0;" value=" ">
    </p>
        </p>
		<p>
              <textarea placeholder=" " style="width: 580px;height: 100px;"></textarea><br>
			   ce certificat est établi et remis en mains propre de l'interesse pour
 faire valoir ce que de droit .
        </p>
        <p style="text-align: right; margin-top: 30px;">
            Dont certificat<br>
            <span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>
        </p>
    </div>
    <div class="print-button" style="display: flex; align-items: center; justify-content: center; gap: 15px;">
        <div style="display: flex; align-items: center; gap: 8px;">
            <label for="fontSize" style="font-size: 14px; margin: 0;">Taille du texte:</label>
            <input type="number" id="fontSize" min="8" max="20" value="14" style="width: 60px; padding: 5px; border: 1px solid #bdbdbd; border-radius: 4px;">
        </div>
        <button id="printButton">Imprimer le Certificat</button>
		<button id="saveButtoncbv">Sauvegarder</button>
    </div>
    
    <script src="certificat-unified-font-size.js"></script>

</body>
</html>
`;

    var newWindow = window.open("", "_blank");
    if (newWindow) {
        newWindow.document.write(certificatContent);
        newWindow.document.close();

        // Attacher l'événement d'impression directement après la fermeture du document
        newWindow.onload = function () {
            const printButton = newWindow.document.getElementById('printButton');
            if (printButton) {
                printButton.addEventListener('click', function () {
                    newWindow.print();
                });
            }

            // Attacher l'événement de sauvegarde
            const saveButton = newWindow.document.getElementById('saveButtoncbv');
            if (saveButton) {
                saveButton.addEventListener('click', function () {
                    sauvegarderCBV(newWindow);
                });
            }
        };
    } else {
        console.log("Popup bloquée par le navigateur.");
    }
}

// Fonction pour sauvegarder le certificat CBV
async function sauvegarderCBV(certificatWindow) {
    try {
        // Récupérer les données du certificat
        const nomPrenomInput = certificatWindow.document.querySelector('input[type="text"][value*=" "]');
        let nom = '', prenom = '';

        if (nomPrenomInput && nomPrenomInput.value) {
            const nomPrenom = nomPrenomInput.value.trim();
            const parts = nomPrenom.split(' ');
            if (parts.length >= 2) {
                nom = parts[0];
                prenom = parts.slice(1).join(' ');
            }
        }

        // Récupérer le médecin
        const medecinInput = certificatWindow.document.getElementById('docteur');
        const medecin = medecinInput ? medecinInput.value.trim() : '';

        // Récupérer la date du certificat
        const today = new Date();
        const dateCertificat = today.toISOString().split('T')[0];

        // Récupérer la date de naissance
        const editableFields = certificatWindow.document.querySelectorAll('.editable-field');
        let dateNaissance = '';

        for (let field of editableFields) {
            const text = field.textContent || field.innerText || '';
            // Chercher un pattern de date (YYYY-MM-DD ou DD/MM/YYYY)
            const dateMatch = text.match(/(\d{4}-\d{2}-\d{2}|\d{2}\/\d{2}\/\d{4}|\d{1,2}\/\d{1,2}\/\d{4})/);
            if (dateMatch) {
                let date = dateMatch[1];
                // Convertir DD/MM/YYYY vers YYYY-MM-DD si nécessaire
                if (date.includes('/')) {
                    const parts = date.split('/');
                    if (parts.length === 3) {
                        // Assumer DD/MM/YYYY
                        date = `${parts[2]}-${parts[1].padStart(2, '0')}-${parts[0].padStart(2, '0')}`;
                    }
                }
                dateNaissance = date;
                break;
            }
        }

        // Récupérer le titre (type d'accident)
        const typeAccidentSelect = certificatWindow.document.getElementById('typeAccident');
        const titre = typeAccidentSelect ? typeAccidentSelect.value : '';

        // Récupérer l'examen (description)
        const descriptionInput = certificatWindow.document.getElementById('descriptionAccident');
        const examen = descriptionInput ? descriptionInput.value.trim() : '';

        // Récupérer l'heure
        const heureInput = certificatWindow.document.getElementById('heureAccident');
        const heure = heureInput && heureInput.value ? heureInput.value : null;

        console.log('📋 Données CBV récupérées:', {
            nom,
            prenom,
            medecin,
            dateCertificat,
            dateNaissance,
            titre,
            examen,
            heure
        });

        // Vérifier que nous avons les données minimales
        if (!nom || !prenom) {
            alert('Erreur: Nom et prénom du patient requis. Veuillez remplir les informations patient d\'abord.');
            return;
        }

        if (!medecin) {
            alert('Erreur: Nom du médecin requis. Veuillez configurer le médecin dans les options.');
            return;
        }

        // Préparer le message pour l'API
        const message = {
            nom: nom,
            prenom: prenom,
            medecin: medecin,
            date_certificat: dateCertificat,
            date_naissance: dateNaissance || null,
            titre: titre || null,
            examen: examen || null,
            heure: heure || null
        };

        console.log('📤 Message à envoyer à l\'API:', message);

        // Envoyer à l'API locale
        try {
            const response = await fetch('http://localhost:5000/api/ajouter_cbv', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(message)
            });
            
            const data = await response.json();
            
            // Afficher les messages dans la fenêtre popup
            if (certificatWindow && !certificatWindow.closed) {
                if (data && data.success) {
                    certificatWindow.alert('CBV sauvegardé avec succès !');
                } else {
                    const errorMsg = data ? data.error : 'Réponse invalide';
                    certificatWindow.alert('Erreur lors de la sauvegarde: ' + errorMsg);
                }
            }
        } catch (error) {
            console.error('❌ Erreur lors de la sauvegarde:', error);
            
            // Afficher les messages dans la fenêtre popup
            if (certificatWindow && !certificatWindow.closed) {
                // Fallback: Afficher les données pour copie manuelle si l'API n'est pas accessible
                if (error.message.includes('Failed to fetch') || error.message.includes('CORS')) {
                    const fallbackMessage = `
API non accessible. Veuillez démarrer le serveur avec: python api_simple.py
Ou copiez ces données manuellement:
${JSON.stringify(message, null, 2)}
                    `;
                    certificatWindow.alert(fallbackMessage);
                } else {
                    certificatWindow.alert('Erreur lors de la sauvegarde: ' + error.message);
                }
            }
        }

    } catch (error) {
        console.error('❌ Erreur lors de la sauvegarde CBV:', error);
        alert('Erreur lors de la sauvegarde: ' + error.message);
    }
}
// Fonction pour générer un certificat d'arrêt de travail
function genererArretTravail() {
    const polyclinique = document.getElementById('polyclinique').value;
    const docteur = document.getElementById('docteur').value;
    
    // Get patient information from the form fields
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '[Nom de l\'élève]';
    const patientAge = document.getElementById('patientAge').value;
    const patientDateNaissance = document.getElementById('patientDateNaissance').value;
    const dateCertificat = document.getElementById('dateCertificat').value || new Date().toISOString().split('T')[0];
    
    // Construire la partie de l'âge/date de naissance
    let ageInfo = '';
    if (patientAge) {
        ageInfo = 'né(e) le ' + patientAge;
    } else if (patientDateNaissance) {
        ageInfo = 'né(e) le ' + patientDateNaissance;
    } else {
        ageInfo = 'né(e) le [Date de naissance]';
    }
    
    // Format the date for display
    const formattedDate = new Date(dateCertificat).toLocaleDateString('fr-FR');
    
    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';
    
    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"></div>';
    }
    
const certificatHtml = `
<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta http-equiv="Content-Security-Policy" content="script-src 'self' 'unsafe-inline' 'unsafe-eval'; object-src 'none';">
<title>Certificat d'arret de Travail</title>
<style>
body {
font-family: Arial, sans-serif;
padding: 20px;
background-color: #f9f9f9;
}
.certificat {
background-color: white;
border: 1px solid #ddd;
padding: 20px;
max-width: 600px;
margin: 0 auto;
margin-top: 60px;
box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
}
h1 {
text-align: center;
color: #333;
text-decoration: underline;
font-size: 20px;
}
p {
line-height: 1.5;
color: #555;
}
.editable-field {
border-bottom: 1px dashed #666;
display: inline-block;
min-width: 50px;
min-height: 20px;
padding: 2px 4px;
margin: 0 3px;
}
.editable-area {
border: 1px solid #ddd;
border-radius: 4px;
padding: 8px;
margin: 5px 0;
width: 100%;
min-height: 20px;
resize: vertical;
overflow: hidden;
font-family: inherit;
font-size: inherit;
line-height: inherit;
}
.editable-area:focus {
outline: none;
border-color: #007bff;
}
#head {
margin-bottom: 20px;
}
#head table {
width: 100%;
border: 0px solid #000000;
padding: 4px;
margin-bottom: 15px;
}
#head td {
text-align: center;
}
.print-button {
text-align: center;
margin-top: 20px;
}
.print-button button {
padding: 10px 20px;
font-size: 16px;
background-color: #007bff;
color: white;
border: none;
border-radius: 5px;
cursor: pointer;
}
.print-button button:hover {
background-color: #0056b3;
}
@media print {
@page {
    size: A5;
    margin: 0.2cm 0.2cm 0.2cm 0.2cm;
}

body {
    margin: 0 !important;
    padding: 0 !important;
    font-size: 10pt !important;
    line-height: 1.2 !important;
    background-color: white;
}

.certificat {
    border: none;
    box-shadow: none;
    margin: 0 !important;
    padding: 2px 8px !important;
    max-width: 100% !important;
}

h1 {
    font-size: 14pt !important;
    margin: 5px 0 !important;
    margin-top: 2cm !important;
}

input[type="text"],
input[type="date"],
textarea {
    border: none !important;
    background: none !important;
    box-shadow: none !important;
    outline: none !important;
    font-size: 9pt !important;
}

input[type="text"]:focus,
input[type="date"]:focus,
textarea:focus {
    border: none !important;
    outline: none !important;
}

.print-button {
    display: none;
}

.editable-field, .editable-area {
    border: none !important;
}

/* Additional space optimization */
* {
    margin-top: 0 !important;
    margin-bottom: 2px !important;
}

p {
    margin: 2px 0 !important;
    font-size: 9pt !important;
}
}
</style>
</head>
<body>
${enteteContent}
<div class="certificat">
<h1>CERTIFICAT MÉDICAL D'ARRÊT DE TRAVAIL</h1>
<br><br><br>
<p>
Je soussigné, Dr <input type="text" id="docteur" value="${docteur}" placeholder="">, certifie avoir examiné ce jour
<strong><input type="text" value="${patientNomPrenom}" style="width: 150px;"></strong>,
<span class="editable-field" contenteditable="true" style="min-width: 100px; display: inline-block;">${ageInfo}</span>.
</p>
<p>
déclare que son état de santé nécessite un arret de travail de
<input type="text" placeholder="1 (un)" style="width: 80px;"> Jour(s)
à daté du <span class="editable-field" contenteditable="true" style="min-width: 120px; display: inline-block;">${formattedDate}</span> <br>
sauf complication.
</p>
<p>
<textarea placeholder=" " style="width: 450px;"></textarea>
</p>
<p style="text-align: right; margin-top: 30px;">
Dont certificat&nbsp&nbsp&nbsp&nbsp&nbsp<br>
<span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp;
</p>
</div>
<div class="print-button" style="display: flex; align-items: center; justify-content: center; gap: 15px;">
    <div style="display: flex; align-items: center; gap: 8px;">
        <label for="fontSize" style="font-size: 14px; margin: 0;">Taille du texte:</label>
        <input type="number" id="fontSize" min="8" max="20" value="14" style="width: 60px; padding: 5px; border: 1px solid #bdbdbd; border-radius: 4px;">
    </div>
    <button id="printButton">Imprimer le Certificat</button>
</div>
<script src="print.js"></script>
<script src="certificat-unified-font-size.js"></script>
<script>
    // Appliquer la taille de police sauvegardée et masquer les éléments non désirés à l'impression
    document.addEventListener('DOMContentLoaded', () => {
        const savedFontSize = localStorage.getItem('certificatFontSize') || '14';
        const styleElement = document.createElement('style');
        styleElement.textContent = "@media print { .print-button { display: none !important; } }";
        styleElement.id = 'certificatFontSizeStyle';
        document.head.appendChild(styleElement);
        
        // Add print functionality
        document.getElementById('printButton').addEventListener('click', function() {
            window.print();
        });
    });
</script>
</body>
</html>
    `;

    var newWindow = window.open("", "_blank");
    if (newWindow) {
        // Stocker la référence de la fenêtre pour le script de sauvegarde
        window.lastOpenedWindow = newWindow;

        // Définir manuellement window.opener pour permettre la communication
        try {
            Object.defineProperty(newWindow, 'opener', {
                value: window,
                writable: false,
                configurable: false
            });
        } catch (e) {
            // Si cela échoue, stocker la référence ailleurs
            newWindow._parentWindow = window;
        }

        newWindow.document.write(certificatHtml);
        newWindow.document.close();

        // Fonction pour effectuer la sauvegarde
        function effectuerSauvegarde(nombreJoursValue) {
            // Récupérer les données depuis les champs de la popup
            const docteurInput = newWindow.document.getElementById('docteur');
            const medecin = docteurInput ? docteurInput.value.trim() : '';

            // Récupérer le nom et prénom
            const nomPrenomInput = newWindow.document.querySelector('strong input[type="text"]');
            let nom = '', prenom = '';
            if (nomPrenomInput && nomPrenomInput.value) {
                const parts = nomPrenomInput.value.trim().split(' ');
                if (parts.length >= 2) {
                    nom = parts[0];
                    prenom = parts.slice(1).join(' ');
                }
            }

            // Vérifier les données
            if (!nom || !prenom) {
                afficherMessageDansPopup('Erreur: Nom et prénom du patient requis.', 'error');
                return;
            }

            if (!medecin) {
                afficherMessageDansPopup('Erreur: Nom du médecin requis.', 'error');
                return;
            }

            // Récupérer la date de naissance depuis le champ editable-field
            const editableFields = newWindow.document.querySelectorAll('.editable-field');
            let dateNaissance = '';
            for (let field of editableFields) {
                const text = field.textContent || field.innerText || '';
                const parentText = field.parentElement ? field.parentElement.textContent || '' : '';
                if (parentText.includes('né(e)') || parentText.includes('né') || parentText.includes('née')) {
                    dateNaissance = text.trim();
                    break;
                }
            }

            if (!dateNaissance && editableFields.length > 0) {
                dateNaissance = (editableFields[0].textContent || editableFields[0].innerText || '').trim();
            }

            // Date du certificat
            const today = new Date();
            const dateCertificat = today.toISOString().split('T')[0];

            // Préparer le message
            const message = {
                action: "ajouter_arret_travail",
                nom: nom,
                prenom: prenom,
                medecin: medecin,
                nombre_jours: parseInt(nombreJoursValue),
                date_certificat: dateCertificat,
                date_naissance: dateNaissance || null
            };

            console.log('📤 Message à envoyer:', message);

            // Utiliser directement la fonction stockée dans la popup
            const sauvegarderFn = window.sauvegarderArretTravailDepuisPopup;

            if (sauvegarderFn && typeof sauvegarderFn === 'function') {
                sauvegarderFn(message).then(response => {
                    if (response && response.success) {  // Changé de response.ok à response.success
                        afficherMessageDansPopup('Arrêt de travail sauvegardé avec succès !', 'success');
                    } else {
                        const errorMsg = response ? response.error : 'Réponse invalide';
                        afficherMessageDansPopup('Erreur lors de la sauvegarde: ' + errorMsg, 'error');
                    }
                }).catch(error => {
                    console.error('❌ Erreur lors de la sauvegarde:', error);
                    afficherMessageDansPopup('Erreur lors de la sauvegarde: ' + error.message, 'error');
                });
            } else {
                setTimeout(() => {
                    const fn = window.sauvegarderArretTravailDepuisPopup;
                    if (fn && typeof fn === 'function') {
                        fn(message).then(response => {
                            if (response && response.success) {  // Changé de response.ok à response.success
                                afficherMessageDansPopup('Arrêt de travail sauvegardé avec succès !', 'success');
                            } else {
                                const errorMsg = response ? response.error : 'Réponse invalide';
                                afficherMessageDansPopup('Erreur lors de la sauvegarde: ' + errorMsg, 'error');
                            }
                        }).catch(error => {
                            console.error('❌ Erreur lors de la sauvegarde:', error);
                            afficherMessageDansPopup('Erreur lors de la sauvegarde: ' + error.message, 'error');
                        });
                    } else {
                        afficherMessageDansPopup('Erreur: Fonction de sauvegarde non disponible. Rechargez la page et réessayez.', 'error');
                    }
                }, 500);
            }
        }

        // Attacher l'événement d'impression directement après la fermeture du document
        newWindow.onload = function () {
            const printButton = newWindow.document.getElementById('printButton');
            if (printButton) {
                const newPrintButton = printButton.cloneNode(true);
                printButton.parentNode.replaceChild(newPrintButton, printButton);

                newPrintButton.addEventListener('click', function () {
                    const joursInputs = newWindow.document.querySelectorAll('input[type="text"]');
                    let nombreJours = '';
                    for (let input of joursInputs) {
                        const parentText = input.parentElement ? input.parentElement.textContent : '';
                        if (parentText.includes('Jour(s)') || parentText.includes('arret de travail')) {
                            nombreJours = input.value.trim();
                            break;
                        }
                    }

                    if (nombreJours) {
                        effectuerSauvegarde(nombreJours);
                    }

                    setTimeout(() => {
                        newWindow.print();
                    }, 500);
                });
            }
        };

        // Fonction pour afficher un message personnalisé dans la popup
        function afficherMessageDansPopup(message, type = 'info') {
            if (newWindow && !newWindow.closed) {
                newWindow.focus();
                newWindow.alert(message);
            }
        }

        // Ajouter le bouton de sauvegarde après que le document soit chargé
        function ajouterBoutonSauvegarde() {
            try {
                const printButton = newWindow.document.getElementById('printButton');
                const saveButton = newWindow.document.getElementById('sauvegarderArretPopup');

                if (printButton && !saveButton) {
                    console.log('✅ Ajout du bouton de sauvegarde dans la popup');

                    const boutonSauvegarde = newWindow.document.createElement('button');
                    boutonSauvegarde.id = 'sauvegarderArretPopup';
                    boutonSauvegarde.innerHTML = '💾 Sauvegarder Arrêt';
                    boutonSauvegarde.style.cssText = 'background-color: #28a745; color: white; border: none; padding: 10px 20px; margin-left: 15px; border-radius: 5px; cursor: pointer; font-size: 16px; transition: background-color 0.3s;';

                    boutonSauvegarde.addEventListener('mouseenter', function () {
                        this.style.backgroundColor = '#218838';
                    });
                    boutonSauvegarde.addEventListener('mouseleave', function () {
                        this.style.backgroundColor = '#28a745';
                    });

                    printButton.parentNode.appendChild(boutonSauvegarde);

                    boutonSauvegarde.addEventListener('click', function () {
                        console.log('💾 Clic sur le bouton de sauvegarde');

                        const joursInputs = newWindow.document.querySelectorAll('input[type="text"]');
                        let nombreJours = '';
                        for (let input of joursInputs) {
                            const parentText = input.parentElement ? input.parentElement.textContent : '';
                            if (parentText.includes('Jour(s)') || parentText.includes('arret de travail')) {
                                nombreJours = input.value.trim();
                                break;
                            }
                        }

                        if (!nombreJours) {
                            const promptDiv = newWindow.document.createElement('div');
                            promptDiv.id = 'promptNombreJours';
                            promptDiv.style.cssText = 'position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%); z-index: 10001; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3); min-width: 300px;';
                            promptDiv.innerHTML = '<div style="margin-bottom: 15px; font-weight: bold; font-size: 16px;">Nombre de jours d\'arrêt de travail</div><input type="number" id="inputNombreJours" value="1" min="1" style="width: 100%; padding: 8px; border: 1px solid #ccc; border-radius: 4px; font-size: 14px; margin-bottom: 15px;"><div style="display: flex; gap: 10px; justify-content: flex-end;"><button id="btnPromptOk" style="padding: 8px 20px; background: #28a745; color: white; border: none; border-radius: 4px; cursor: pointer;">OK</button><button id="btnPromptCancel" style="padding: 8px 20px; background: #6c757d; color: white; border: none; border-radius: 4px; cursor: pointer;">Annuler</button></div>';

                            const overlay = newWindow.document.createElement('div');
                            overlay.id = 'promptOverlay';
                            overlay.style.cssText = 'position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0, 0, 0, 0.5); z-index: 10000;';

                            newWindow.document.body.appendChild(overlay);
                            newWindow.document.body.appendChild(promptDiv);

                            setTimeout(() => {
                                const input = newWindow.document.getElementById('inputNombreJours');
                                if (input) {
                                    input.focus();
                                    input.select();
                                }
                            }, 100);

                            const btnOk = newWindow.document.getElementById('btnPromptOk');
                            const btnCancel = newWindow.document.getElementById('btnPromptCancel');

                            const cleanup = () => {
                                if (promptDiv.parentNode) promptDiv.parentNode.removeChild(promptDiv);
                                if (overlay.parentNode) overlay.parentNode.removeChild(overlay);
                            };

                            btnOk.addEventListener('click', () => {
                                const value = newWindow.document.getElementById('inputNombreJours').value;
                                cleanup();
                                if (value && !isNaN(value) && parseInt(value) > 0) {
                                    effectuerSauvegarde(value);
                                } else {
                                    afficherMessageDansPopup('Veuillez entrer un nombre de jours valide', 'warning');
                                }
                            });

                            btnCancel.addEventListener('click', () => {
                                cleanup();
                            });

                            const input = newWindow.document.getElementById('inputNombreJours');
                            input.addEventListener('keydown', (e) => {
                                if (e.key === 'Enter') {
                                    btnOk.click();
                                } else if (e.key === 'Escape') {
                                    btnCancel.click();
                                }
                            });
                        } else {
                            if (!nombreJours || isNaN(nombreJours) || parseInt(nombreJours) <= 0) {
                                afficherMessageDansPopup('Veuillez entrer un nombre de jours valide', 'warning');
                                return;
                            }
                            effectuerSauvegarde(nombreJours);
                        }
                    });

                    console.log('✅ Bouton de sauvegarde ajouté avec succès');
                } else if (!printButton) {
                    console.log('⏳ Bouton printButton pas encore disponible, réessai...');
                    setTimeout(ajouterBoutonSauvegarde, 100);
                } else if (saveButton) {
                    console.log('ℹ️ Bouton de sauvegarde déjà présent');
                }
            } catch (e) {
                console.error('❌ Erreur lors de l\'ajout du bouton:', e);
            }
        }

        // Créer une fonction de sauvegarde dans la fenêtre parent
        const sauvegarderFn = async function (message) {
            console.log('🔗 Sauvegarde depuis popup, message:', message);
            
            try {
                // Appeler l'API locale Python
                const response = await fetch('http://localhost:5000/api/ajouter_arret_travail', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify(message)
                });
                
                const result = await response.json();
                
                if (result.success) {
                    console.log('✅ Arrêt de travail sauvegardé avec succès:', result.message);
                    
                    // Afficher un message de succès discret
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
                    successDiv.textContent = result.message;
                    document.body.appendChild(successDiv);
                    
                    // Supprimer le message après 3 secondes
                    setTimeout(() => {
                        if (successDiv.parentNode) {
                            successDiv.parentNode.removeChild(successDiv);
                        }
                    }, 3000);
                    
                    return { success: true, message: result.message };
                } else {
                    console.error('❌ Erreur lors de la sauvegarde:', result.error);
                    return { success: false, error: result.error };
                }
                
            } catch (error) {
                console.error('❌ Erreur réseau:', error);
                return { success: false, error: 'Erreur de connexion à l\'API locale. Vérifiez que le serveur Python est démarré.' };
            }
        };

        // Stocker la fonction de sauvegarde
        window.sauvegarderArretTravailDepuisPopup = sauvegarderFn;

        // Ajouter le bouton de sauvegarde après un léger délai
        setTimeout(ajouterBoutonSauvegarde, 200);
    } else {
        console.log("Popup bloquée par le navigateur.");
    }
}

function setupFormatButtons() {
    const formatAvecEnteteBtn = document.getElementById("formatAvecEntete");
    const formatSansEnteteBtn = document.getElementById("formatSansEntete");
    
    if (formatAvecEnteteBtn) {
        formatAvecEnteteBtn.addEventListener("click", function() {
            localStorage.setItem('certificatFormat', 'avecEntete');
            this.classList.add('selected-format');
            if (formatSansEnteteBtn) {
                formatSansEnteteBtn.classList.remove('selected-format');
            }
        });
    }
    
    if (formatSansEnteteBtn) {
        formatSansEnteteBtn.addEventListener("click", function() {
            localStorage.setItem('certificatFormat', 'sansEntete');
            this.classList.add('selected-format');
            if (formatAvecEnteteBtn) {
                formatAvecEnteteBtn.classList.remove('selected-format');
            }
        });
    }
}

// Configurer les gestionnaires d'événements lorsque le DOM est chargé
document.addEventListener('DOMContentLoaded', function() {
    loadData();
    setupFormatButtons();
    
    // Initialiser le format au chargement
    const format = localStorage.getItem('certificatFormat');
    const formatAvecEnteteBtn = document.getElementById('formatAvecEntete');
    const formatSansEnteteBtn = document.getElementById('formatSansEntete');
    
    if (format === 'sansEntete' && formatSansEnteteBtn) {
        formatSansEnteteBtn.classList.add('selected-format');
    } else if (formatAvecEnteteBtn) {
        // Par défaut, on utilise avec en-tete
        formatAvecEnteteBtn.classList.add('selected-format');
    }
    
    // Écouteurs pour les boutons
    const saveBtn = document.getElementById("SavePolycliniqueDocteur");
    if (saveBtn) {
        saveBtn.addEventListener("click", saveData);
    }
    
    const certificatBtn = document.getElementById("genererCertificat");
    if (certificatBtn) {
        certificatBtn.addEventListener("click", genererCertificat);
    }
 
    const inaptSportBtn = document.getElementById("inaptSport");
    if (inaptSportBtn) {
        inaptSportBtn.addEventListener("click", inaptitudeSport);
    }
    
    const arretBtn = document.getElementById("genererArret");
    if (arretBtn) {
        arretBtn.addEventListener("click", genererArretTravail);
    }
    
    const radioxBtn = document.getElementById("genererRadiox");
    if (radioxBtn) {
        radioxBtn.addEventListener("click", genererRadiox);
    }
    
    // Écouteur pour le champ date de naissance - calcul automatique de l'âge
    const dateNaissanceInput = document.getElementById('patientDateNaissance');
    if (dateNaissanceInput) {
        dateNaissanceInput.addEventListener('change', function() {
            const dateNaissance = this.value;
            if (dateNaissance) {
                const ageCalcule = calculerAge(dateNaissance);
                const ageInput = document.getElementById('patientAge');
                if (ageInput) {
                    ageInput.value = ageCalcule;
                }
            }
        });
    }
    
    // Écouteur pour le champ âge - effacer la date de naissance si l'âge est modifié manuellement
    const ageInput = document.getElementById('patientAge');
    if (ageInput) {
        ageInput.addEventListener('input', function() {
            // Si l'utilisateur commence à taper dans le champ âge, on ne force plus le calcul automatique
// Mais on ne vide la date de naissance que si l'âge est significativement différent
            // Pour permettre les deux modes de saisie
        });
    }
});

// bilan Leishmaniose
function ouvrirCertificatLeishmaniose() {
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '';
    const patientDateNaissance = document.getElementById('patientDateNaissance').value;
    
    // Extraire nom et prénom
    const nomPrenomArray = patientNomPrenom.split(' ');
    const nom = nomPrenomArray[nomPrenomArray.length - 1] || '';
    const prenom = nomPrenomArray.slice(0, -1).join(' ') || '';
    
    const patientInfo = {
        nom: nom,
        prenom: prenom,
        dob: patientDateNaissance
    };

    const today = new Date();
    const year = today.getFullYear();
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const day = String(today.getDate()).padStart(2, '0');
    const todayFormatted = `${year}-${month}-${day}`;

    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";
    const docteur = localStorage.getItem('docteur') || "";

    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';

    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"></div>';
    }

    const certificatContent = `
<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Etude microscopique de leishmaniae</title>
<style>
body {
font-family: Arial, sans-serif;
padding: 20px;
background-color: #f9f9f9;
}
.certificat {
background-color: white;
border: 1px solid #ddd;
padding: 20px;
max-width: 600px;
margin: 0 auto;
margin-top: 60px;
box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
}
h1 {
text-align: center;
color: #333;
text-decoration: underline;
font-size: 20px;
}
p {
line-height: 1.5;
color: #555;
}
.editable-field {
border-bottom: 1px dashed #666;
display: inline-block;
min-width: 50px;
min-height: 20px;
padding: 2px 4px;
margin: 0 3px;
}
.editable-area {
border: 1px solid #ddd;
border-radius: 4px;
padding: 8px;
margin: 5px 0;
width: 100%;
min-height: 20px;
resize: vertical;
overflow: hidden;
font-family: inherit;
font-size: inherit;
line-height: inherit;
}
.editable-area:focus {
outline: none;
border-color: #007bff;
}
#head {
margin-bottom: 20px;
}
#head table {
width: 100%;
border: 0px solid #000000;
padding: 4px;
margin-bottom: 15px;
}
#head td {
text-align: center;
}
.print-button {
text-align: center;
margin-top: 20px;
}
.print-button button {
padding: 10px 20px;
font-size: 16px;
background-color: #007bff;
color: white;
border: none;
border-radius: 5px;
cursor: pointer;
}
.print-button button:hover {
background-color: #0056b3;
}
@media print {
@page {
size: A5;
margin: 0.2cm 0.2cm 0.2cm 0.2cm;
}
body {
margin: 0 !important;
padding: 0 !important;
font-size: 10pt !important;
background-color: white;
}
.certificat {
padding: 2px 8px !important;
max-width: 100% !important;
border: none;
box-shadow: none;
margin-top: 0;
}
h1 {
font-size: 14pt !important;
margin: 5px 0 !important;
margin-top: 2cm !important;
}
p {
font-size: 9pt !important;
margin: 2px 0 !important;
line-height: 1.2 !important;
}
input[type="text"],
input[type="date"],
textarea {
border: none !important;
background: none !important;
box-shadow: none !important;
outline: none !important;
font-size: 9pt !important;
}
input[type="text"]:focus,
input[type="date"]:focus,
textarea:focus {
border: none !important;
outline: none !important;
}
/* Rendre le placeholder transparent lors de l'impression */
input::placeholder,
textarea::placeholder {
color: transparent;
}
.print-button {
display: none;
}
.editable-field, .editable-area {
border: none !important;
}
.docteur {
font-weight: bold;
font-size: 14pt !important;
margin-right: 50px;
}
}
</style>
</head>
<body>
${enteteContent}
<div class="certificat">
<h1>Cher confrère.</h1>
<div class="contenu-certificat" style="margin-top: 1.5cm !important;">
<p>
<br>
Permettez-moi de vous adresser le(a) nommé(e) <br>
<strong><input type="text" value="${nom} ${prenom}" style="width: 400px;"></strong>, <br>
Pour étude microscopique à  la recherche du corps de leishmanies sur les lésions :<br>
<textarea placeholder="Description des lésions" style="width: 100%;"></textarea>
</p>
<p style="text-align: right;">
Confraternellement,<br>
<span class="docteur">Dr ${docteur}</span>&nbsp;
</p>
</div>
</div>
<div class="print-button" style="display: flex; align-items: center; justify-content: center; gap: 15px;">
    <div style="display: flex; align-items: center; gap: 8px;">
        <label for="fontSize" style="font-size: 14px; margin: 0;">Taille du texte:</label>
        <input type="number" id="fontSize" min="8" max="20" value="14" style="width: 60px; padding: 5px; border: 1px solid #bdbdbd; border-radius: 4px;">
    </div>
    <button id="printButton">Imprimer la lettre </button>
</div>
<script src="print.js"></script>
<script src="certificat-unified-font-size.js"></script>
</body>
</html>
`;

    const newWindow = window.open("", "_blank");
    newWindow.document.write(certificatContent);
    newWindow.document.close();
}

// Fonction pour générer un certificat de bonne santé
function genererBonSante() {
    // Get patient information from the form fields
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '[Nom du patient]';
    const patientAge = document.getElementById('patientAge').value;
    const patientDateNaissance = document.getElementById('patientDateNaissance').value;
    const dateCertificat = document.getElementById('dateCertificat').value || new Date().toISOString().split('T')[0];
    
    // Construire la partie de l'âge/date de naissance
    let ageInfo = '';
    if (patientAge) {
        ageInfo = 'âgé(e) de ' + patientAge;
    } else if (patientDateNaissance) {
        ageInfo = 'né(e) le ' + patientDateNaissance;
    } else {
        ageInfo = 'né(e) le [Date de naissance]';
    }
    
    // Format the date for display
    const formattedDate = new Date(dateCertificat).toLocaleDateString('fr-FR');
    
    const polyclinique = localStorage.getItem('polyclinique') || "";
    const docteur = localStorage.getItem('docteur') || "";

    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';

    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"></div>';
    }

    const certificatContent = `
<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Certificat de Bonne Santé</title>
    <style>
body {
font-family: Arial, sans-serif;
padding: 20px;
background-color: #f9f9f9;
}
.certificat {
background-color: white;
border: 1px solid #ddd;
padding: 20px;
max-width: 600px;
margin: 0 auto;
margin-top: 60px;
box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
}
h1 {
text-align: center;
color: #333;
text-decoration: underline;
font-size: 20px;
}
p {
line-height: 1.5;
color: #555;
}
.editable-field {
border-bottom: 1px dashed #666;
display: inline-block;
min-width: 50px;
min-height: 20px;
padding: 2px 4px;
margin: 0 3px;
}
.editable-area {
border: 1px solid #ddd;
border-radius: 4px;
padding: 8px;
margin: 5px 0;
width: 100%;
min-height: 20px;
resize: vertical;
overflow: hidden;
font-family: inherit;
font-size: inherit;
line-height: inherit;
}
.editable-area:focus {
outline: none;
border-color: #007bff;
}
#head {
margin-bottom: 20px;
}
#head table {
width: 100%;
border: 0px solid #000000;
padding: 4px;
margin-bottom: 15px;
}
#head td {
text-align: center;
}
.print-button {
text-align: center;
margin-top: 20px;
}
.print-button button {
padding: 10px 20px;
font-size: 16px;
background-color: #007bff;
color: white;
border: none;
border-radius: 5px;
cursor: pointer;
}
.print-button button:hover {
background-color: #0056b3;
}
@media print {
@page {
    size: A5;
    margin: 0.2cm 0.2cm 0.2cm 0.2cm;
}

body {
    margin: 0 !important;
    padding: 0 !important;
    font-size: 10pt !important;
    line-height: 1.2 !important;
    background-color: white;
}

.certificat {
    border: none;
    box-shadow: none;
    margin: 0 !important;
    padding: 2px 8px !important;
    max-width: 100% !important;
}

h1 {
    font-size: 14pt !important;
    margin: 5px 0 !important;
    margin-top: 2cm !important;
}

input[type="text"],
input[type="date"],
textarea {
    border: none !important;
    background: none !important;
    box-shadow: none !important;
    outline: none !important;
    font-size: 9pt !important;
}

input[type="text"]:focus,
input[type="date"]:focus,
textarea:focus {
    border: none !important;
    outline: none !important;
}

/* Styles existants */
.print-button {
    display: none;
}
.editable-field, .editable-area {
    border: none !important;
}

/* Additional space optimization */
* {
    margin-top: 0 !important;
    margin-bottom: 2px !important;
}

p {
    margin: 2px 0 !important;
    font-size: 9pt !important;
}
}
</style>
</head>
<body>
${enteteContent}
    <div class="certificat">
        <h1>CERTIFICAT DE BONNE SANTÉ</h1>
        <div class="contenu-certificat" style="margin-top: 1.5cm !important;">
        <p>
            Je soussigné, Dr <input type="text" id="docteur" value="${docteur}" placeholder="" style="width: 120px;">, 
            certifie avoir examiné ce jour le(la) susnommé(e) 
            <strong><input type="text" value="${patientNomPrenom}" style="width: 180px;"></strong>,
            <span class="editable-field" contenteditable="true" style="min-width: 100px; display: inline-block;">${ageInfo}</span>.
        </p>
        <p>
            Déclare que son état de santé est bon et qu'il/elle est apte à exercer ses activités normales.
        </p>
        <p>
            Ce certificat est délivré à la demande de l'intéressé(e) et remis en main propre pour servir et valoir ce que de droit.
        </p>
        <p style="text-align: right; margin-top: 30px;">
            Fait à <strong>${polyclinique || '[Lieu]'}</strong>, le <strong>${formattedDate}</strong><br><br>
            Dont certificat<br>
            <span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
        </p>
    </div>
    <div class="print-button" style="display: flex; align-items: center; justify-content: center; gap: 15px;">
        <div style="display: flex; align-items: center; gap: 8px;">
            <label for="fontSize" style="font-size: 14px; margin: 0;">Taille du texte:</label>
            <input type="number" id="fontSize" min="8" max="20" value="14" style="width: 60px; padding: 5px; border: 1px solid #bdbdbd; border-radius: 4px;">
        </div>
        <button id="printButton">Imprimer le Certificat</button>
    </div>
    <script>
    // Fonction pour imprimer le certificat
    document.getElementById('printButton').addEventListener('click', function() {
        window.print();
    });
    </script>
</body>
</html>
`;

    const newWindow = window.open("", "_blank");
    newWindow.document.write(certificatContent);
    newWindow.document.close();
}

// Fonction pour générer une requisition
function genererRequisition() {
    console.log("Fonction genererRequisition appelée");
    
    // Récupérer les informations du patient
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '';
    const patientAge = document.getElementById('patientAge').value || '';
    const patientDateNaissance = document.getElementById('patientDateNaissance').value || '';
    
    // Créer les informations du patient
    const patientInfo = {
        nom: patientNomPrenom.split(' ')[0] || '',
        prenom: patientNomPrenom.split(' ').slice(1).join(' ') || '',
        age: patientAge,
        dateNaissance: patientDateNaissance,
        numero: Math.random().toString(36).substr(2, 9) // Générer un numéro aléatoire
    };

    const modal = document.createElement('div');
    modal.className = 'modal';
    modal.innerHTML = `
    <div class="modal-content">
        <h3>Requisition Médicale</h3>
        <div class="info barcode" style="height: 80px;">
            <svg id="barcode" data-numero="${patientInfo.numero || ''}" style="height: 100%;"></svg>
        </div>
        <div class="button-group">
            <button class="modal-button" id="requisitionApte">Apte pour garde à  vue</button>
            <button class="modal-button" id="requisitionInapte">Inapte pour garde à  vue</button>
        </div>
    </div>
    `;

    document.body.appendChild(modal);
    
    // Ecouteur pour le bouton requisitionApte
    document.querySelector('#requisitionApte').addEventListener('click', () => {
        requisitionApte(); // Ouvre la modale de choix Zagreb ou Essens
    });
    
    // Ecouteur pour le bouton requisitionInapte
    document.querySelector('#requisitionInapte').addEventListener('click', () => {
        requisitionInapte(); // Appelle la fonction Tissulairesanssar
    });

    // Ajouter un écouteur de clic pour fermer la modale
    modal.addEventListener('click', function (event) {
        // Si l'utilisateur clique en dehors du contenu de la modale
        if (event.target === modal) {
            modal.remove();
            // Rafraîchir la page
            window.location.reload();
        }
    });

    // Ajouter le style pour la popup
    const style = document.createElement('style');
    style.textContent = `
    .modal {
        position: fixed;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        background-color: rgba(0, 0, 0, 0.5);
        display: flex;
        justify-content: center;
        align-items: center;
        z-index: 1000;
    }
    .modal-content {
        background-color: white;
        padding: 20px;
        border-radius: 5px;
        box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
        text-align: center;
    }
    .button-group {
        display: flex;
        gap: 10px;
        justify-content: center;
        margin-top: 20px;
    }
    .modal-button {
        padding: 10px 20px;
        border: none;
        border-radius: 4px;
        cursor: pointer;
        font-size: 16px;
    }
    .modal-button:first-child {
        background-color: #4CAF50;
        color: white;
        z-index: 1000;
    }
    .modal-content {
        background-color: white;
        padding: 20px;
        border-radius: 8px;
        max-width: 400px;
        width: 90%;
        text-align: center;
    }
    .button-group {
        display: flex;
        gap: 10px;
        margin-top: 20px;
    }
    .button-group button {
        flex: 1;
        padding: 12px;
        border: none;
        border-radius: 5px;
        cursor: pointer;
        font-size: 16px;
        transition: background-color 0.3s;
    }
    .button-group button:hover {
        background-color: #f0f0f0;
    }
    `;
    document.head.appendChild(style);
}

// Fonctions pour la requisition
function requisitionApte() {
    // Fermer la modale si elle existe
    const existingModal = document.querySelector('.modal');
    if (existingModal) {
        existingModal.remove();
        window.location.reload();
    }

    // Get patient information from the new fields
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '';
    const patientAge = document.getElementById('patientAge').value;
    const patientDateNaissance = document.getElementById('patientDateNaissance').value;
    const dateCertificat = document.getElementById('dateCertificat').value || new Date().toISOString().split('T')[0];
    
    // Construire la partie de l'âge/date de naissance
    let ageInfo = '';
    let dob = '';
    if (patientAge) {
        ageInfo = patientAge;
        dob = patientAge; // Utiliser l'âge si pas de date de naissance
    } else if (patientDateNaissance) {
        ageInfo = patientDateNaissance;
        dob = patientDateNaissance;
    } else {
        ageInfo = '[Date de naissance]';
        dob = '[Date de naissance]';
    }

    const today = new Date();
    const year = today.getFullYear();
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const day = String(today.getDate()).padStart(2, '0');
    const todayFormatted = `${year}-${month}-${day}`;

    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";
    const docteur = localStorage.getItem('docteur') || "";
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';
    let enteteContent = avecEntete ? generateHeader() : '<div style="height: 155px;"></div>';

    const certificatContent = `
<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8">
  <title>Requisition Apte</title>
  <style>
    body {
      font-family: Arial, sans-serif;
      padding: 20px;
      background-color: #f9f9f9;
    }
    .certificat {
      background-color: white;
      border: 1px solid #ddd;
      padding: 20px;
      max-width: 600px;
      margin: 0 auto;
      margin-top: 60px;
      box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
    }
    h1 {
      text-align: center;
      color: #333;
      text-decoration: underline;
      font-size: 20px;
    }
    p {
      line-height: 1.5;
      color: #555;
    }
    input[readonly] {
      border: none;
      background: transparent;
	  
    }
    textarea.auto-expand {
      overflow: hidden;
       border: none;
   
      transition: height 0.2s ease;
      min-height: 20px;
      width: 100%;
      font-family: inherit;
      font-size: 14px;
    }
    .print-button {
      text-align: center;
      margin-top: 20px;
    }
    .print-button button {
      padding: 10px 20px;
      font-size: 16px;
      background-color: #007bff;
      color: white;
      border: none;
      border-radius: 5px;
      cursor: pointer;
    }
    .print-button button:hover {
      background-color: #0056b3;
    }
    @media print {
      @page {
        size: A5;
        margin: 0.2cm 0.2cm 0.2cm 0.2cm;
      }
      body {
        margin: 0 !important;
        padding: 0 !important;
        font-size: 10pt !important;
        background-color: white;
      }
      .certificat {
        padding: 2px 8px !important;
        max-width: 100% !important;
        border: none;
        box-shadow: none;
        margin-top: 0;
      }
      h1 {
        font-size: 14pt !important;
        margin: 2cm 0 5px 0 !important;
      }
      p {
        font-size: 9pt !important;
        margin: 2px 0 !important;
        line-height: 1.2 !important;
      }
      input[type="text"],
      input[type="date"],
      textarea {
        border: none !important;
        background: none !important;
        box-shadow: none !important;
        outline: none !important;
        font-size: 9pt !important;
      }
      input[type="text"]:focus,
      input[type="date"]:focus,
      textarea:focus {
        border: none !important;
        outline: none !important;
      }
      ::placeholder {
        color: transparent !important;
      }
      .print-button {
        display: none;
      }
      .docteur {
        font-weight: bold;
        font-size: 14pt !important;
        margin-right: 50px;
      }
      /* Additional space optimization */
      * {
        margin-top: 0 !important;
        margin-bottom: 2px !important;
      }
    }
  </style>
</head>
<body>
  ${enteteContent}
  <div class="certificat">
    <h1>CERTIFICAT MEDICAL</h1>
    <div class="contenu-certificat" style="margin-top: 1.5cm !important;">
    <p>
      Je soussigné(e), Dr 
      <input type="text" value="${docteur}" readonly style="width: 120px;">, 
      certifie avoir examiné ce jour le nomee 
      <strong><input type="text" value="${patientNomPrenom}" readonly placeholder="Nom et prénom" style="width: 180px; padding: 4px; border: 1px solid #ddd; border-radius: 4px; margin: 0 5px;"></strong>
      né(e) le 
      <strong><input type="text" value="${dob}" readonly style="width: 100px;"></strong>, 
      suite à  la réquisition numéro 
      <input type="text" placeholder="Numéro de réquisition" style="width: 240px;">
    </p>
    <p>
      Après un examen médical :<br>
      <textarea class="auto-expand" placeholder=" "></textarea><br>
      Je déclare que le sus nommé est compatible avec les conditions de garde à  vue. Le présent certificat est remis à  l'autorité pour servir et valoir ce que de droit.
    </p>
    <p style="text-align: right; margin-top: 30px;">
      Dont certificat<br>
      <span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>
    </p>
  </div>
 
  <div class="print-button">
<button id="printButton">Imprimer le Certificat</button>

</div>
<script src="print.js"></script>
</body>
</html>
`;

    const newWindow = window.open("", "_blank");
    if (newWindow) {
        newWindow.document.write(certificatContent);
        newWindow.document.close();
        newWindow.onload = function () {
            const modal = document.querySelector('div[style*="position: fixed;"]');
            if (modal) document.body.removeChild(modal);

            const printButton = newWindow.document.getElementById('printButton');
            if (printButton) {
                printButton.addEventListener('click', function () {
                    newWindow.print();
                });
            }
        };
    } else {
        console.log("Popup bloquée par le navigateur.");
    }
}

function requisitionInapte() {
    // Fermer la modale si elle existe
    const existingModal = document.querySelector('.modal');
    if (existingModal) {
        existingModal.remove();
        window.location.reload();
    }

    // Get patient information from the new fields
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '';
    const patientAge = document.getElementById('patientAge').value;
    const patientDateNaissance = document.getElementById('patientDateNaissance').value;
    const dateCertificat = document.getElementById('dateCertificat').value || new Date().toISOString().split('T')[0];
    
    // Construire la partie de l'âge/date de naissance
    let ageInfo = '';
    let dob = '';
    if (patientAge) {
        ageInfo = patientAge;
        dob = patientAge; // Utiliser l'âge si pas de date de naissance
    } else if (patientDateNaissance) {
        ageInfo = patientDateNaissance;
        dob = patientDateNaissance;
    } else {
        ageInfo = '[Date de naissance]';
        dob = '[Date de naissance]';
    }

    const today = new Date();
    const year = today.getFullYear();
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const day = String(today.getDate()).padStart(2, '0');
    const todayFormatted = `${year}-${month}-${day}`;

    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";
    const docteur = localStorage.getItem('docteur') || "";
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';
    let enteteContent = avecEntete ? generateHeader() : '<div style="height: 155px;"></div>';

    const certificatContent = `
<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8">
  <title>Requisition Inapte</title>
  <style>
    body {
      font-family: Arial, sans-serif;
      padding: 20px;
      background-color: #f9f9f9;
    }
    .certificat {
      background-color: white;
      border: 1px solid #ddd;
      padding: 20px;
      max-width: 600px;
      margin: 0 auto;
      margin-top: 20px;
      box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
    }
    h1 {
      text-align: center;
      color: #333;
      text-decoration: underline;
      font-size: 20px;
    }
    p {
      line-height: 1.4;
      color: #555;
    }
    input[readonly] {
      border: none;
      background: transparent;
	  
    }
    textarea.auto-expand {
      overflow: hidden;
       border: none;
   
      transition: height 0.2s ease;
      min-height: 5px;
      width: 100%;
      font-family: inherit;
      font-size: 14px;
    }
    .print-button {
      text-align: center;
      margin-top: 20px;
    }
    .print-button button {
      padding: 10px 20px;
      font-size: 16px;
      background-color: #007bff;
      color: white;
      border: none;
      border-radius: 5px;
      cursor: pointer;
    }
    .print-button button:hover {
      background-color: #0056b3;
    }
    @media print {
      @page {
        size: A5;
        margin: 0.2cm 0.2cm 0.2cm 0.2cm;
      }
      body {
        margin: 0 !important;
        padding: 0 !important;
        font-size: 10pt !important;
        background-color: white;
      }
      .certificat {
        padding: 2px 8px !important;
        max-width: 100% !important;
        border: none;
        box-shadow: none;
        margin-top: 0;
      }
      h1 {
        font-size: 14pt !important;
        margin: 5px 0 !important;
        margin-top: 2cm !important;
      }
      p {
        font-size: 9pt !important;
        margin: 2px 0 !important;
        line-height: 1.2 !important;
      }
      input[type="text"],
      input[type="date"],
      textarea {
        border: none !important;
        background: none !important;
        box-shadow: none !important;
        outline: none !important;
        font-size: 9pt !important;
      }
      input[type="text"]:focus,
      input[type="date"]:focus,
      textarea:focus {
        border: none !important;
        outline: none !important;
      }
      ::placeholder {
        color: transparent !important;
      }
      .print-button {
        display: none;
      }
      .docteur {
        font-weight: bold;
        font-size: 14pt !important;
        margin-right: 50px;
      }
      /* Additional space optimization */
      * {
        margin-top: 0 !important;
        margin-bottom: 2px !important;
      }
    }
  </style>
</head>
<body>
  ${enteteContent}
  <div class="certificat">
    <h1>CERTIFICAT MEDICAL D'INAPTITUDE AU GARDE-à-VUE</h1>
    <div class="contenu-certificat" style="margin-top: 1.5cm !important;">
    <p>
      Je soussigné(e), Dr 
      <input type="text" value="${docteur}" readonly style="width: 120px;">, 
      certifie avoir examiné ce jour le nomee 
      <strong><input type="text" value="${patientNomPrenom}" readonly placeholder="Nom et prénom" style="width: 180px; padding: 4px; border: 1px solid #ddd; border-radius: 4px; margin: 0 5px;"></strong>
      né(e) le 
      <strong><input type="text" value="${dob}" readonly style="width: 100px;"></strong>, 
      suite à  la réquisition numéro 
      <input type="text" placeholder="Numéro de réquisition" style="width: 240px;"><br>
    
              Après un examen clinique :<br>

Je déclare que le(a) susnommé(e) présente des contre-indications à  la garde à  vue pour les raisons suivantes :<br>

<textarea style="width: 100%;  margin-top: 10px;" placeholder="Décrire brièvement les contre-indications"></textarea><br>

En conséquence, je recommande qu'il/elle ne soit pas soumis(e) à  la garde à  vue.<br>

Le présent certificat est remis à  l'autorité compétente pour servir et valoir ce que de droit.


    </p>
    <p style="text-align: right; margin-top: 30px;">
      Dont certificat<br>
      <span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>
    </p>
  </div>
 
  <div class="print-button">
<button id="printButton">Imprimer le Certificat</button>

</div>
<script src="print.js"></script>
</body>
</html>
`;

    const newWindow = window.open("", "_blank");
    if (newWindow) {
        newWindow.document.write(certificatContent);
        newWindow.document.close();
        newWindow.onload = function () {
            const modal = document.querySelector('div[style*="position: fixed;"]');
            if (modal) document.body.removeChild(modal);

            const printButton = newWindow.document.getElementById('printButton');
            if (printButton) {
                printButton.addEventListener('click', function () {
                    newWindow.print();
                });
            }
        };
    } else {
        console.log("Popup bloquée par le navigateur.");
    }
}


//cat Leishmaniose inf a 03 lesions

function ouvrirCertificatLeishmanioseDetail() {
	  const nom = document.getElementById('patientNomPrenom').value || '';
    const dob = document.getElementById('patientDateNaissance').value;
    

    const today = new Date();
    const year = today.getFullYear();
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const day = String(today.getDate()).padStart(2, '0');
    const todayFormatted = `${year}-${month}-${day}`;

    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";
    const docteur = localStorage.getItem('docteur') || "";


    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';

    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"></div>';
    }

    const certificatContent = `
<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Cat de Leishmaniose</title>
<style>
body {
font-family: Arial, sans-serif;
padding: 20px;
background-color: #f9f9f9;
}
.certificat {
background-color: white;
border: 1px solid #ddd;
padding: 20px;
max-width: 600px;
margin: 0 auto;
margin-top: 60px;
box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
}
h1 {
text-align: center;
color: #333;
text-decoration: underline;
font-size: 20px;
}
p {
line-height: 1.5;
color: #555;
}
.editable-field {
border-bottom: 1px dashed #666;
display: inline-block;
min-width: 50px;
min-height: 20px;
padding: 2px 4px;
margin: 0 3px;
}
.editable-area {
border: 1px solid #ddd;
border-radius: 4px;
padding: 8px;
margin: 5px 0;
width: 100%;
min-height: 20px;
resize: vertical;
overflow: hidden;
font-family: inherit;
font-size: inherit;
line-height: inherit;
}
.editable-area:focus {
outline: none;
border-color: #007bff;
}
#head {
margin-bottom: 20px;
}
#head table {
width: 100%;
border: 0px solid #000000;
padding: 4px;
margin-bottom: 15px;
}
#head td {
text-align: center;
}
.print-button {
text-align: center;
margin-top: 20px;
}
.print-button button {
padding: 10px 20px;
font-size: 16px;
background-color: #007bff;
color: white;
border: none;
border-radius: 5px;
cursor: pointer;
}
.print-button button:hover {
background-color: #0056b3;
}
@media print {
@page {
size: A5;
margin: 0.2cm 0.2cm 0.2cm 0.2cm;
}
body {
margin: 0 !important;
padding: 0 !important;
font-size: 10pt !important;
background-color: white;
}
.certificat {
padding: 2px 8px !important;
max-width: 100% !important;
border: none;
box-shadow: none;
margin-top: 0;
}
h1 {
font-size: 14pt !important;
margin: 5px 0 !important;
}
p {
font-size: 9pt !important;
margin: 2px 0 !important;
line-height: 1.2 !important;
}
input[type="text"],
input[type="date"],
textarea {
border: none !important;
background: none !important;
box-shadow: none !important;
outline: none !important;
font-size: 9pt !important;
}
input[type="text"]:focus,
input[type="date"]:focus,
textarea:focus {
border: none !important;
outline: none !important;
}
.print-button {
display: none;
}
.editable-field, .editable-area {
border: none !important;
}
.docteur {
font-weight: bold;
font-size: 14pt !important;
margin-right: 50px;
}
}
</style>
</head>
<body>
${enteteContent}
<div class="certificat">
<h1>Prière de faire</h1>
<div class="contenu-certificat" style="margin-top: 1.5cm !important;">
<p>
Infiltrations locales pour le(a) nommé(e) <br><strong><input type="text" value="${nom}" style="width: 200px;"></strong>  de <input type="text" value="02" size="2" />cc du Glucantime<br>
(pour chaque lésion)<br>
<input type="text" value="02" size="1" /> fois par semaine à  01 cm de bords de(s) lésion(s)<br>
pendant <input type="text" value="04" size="2" /> semaines<br>
</p>
<p>
(selon l'instruction N06 du 16 oct 2011 relative à  la catégorie de leishmaniose cutanée)
</p>
<p style="text-align: right;">
Signature de médecin,<br>
<span class="docteur">Dr ${docteur}</span>&nbsp;
</p>
</div>
</div>
<div class="print-button" style="display: flex; align-items: center; justify-content: center; gap: 15px;">
    <div class="print-controls" style="display: flex; align-items: center; gap: 8px;">
        <label for="fontSize1" style="font-size: 14px; margin: 0;">Taille du texte:</label>
        <input type="number" id="fontSize1" min="8" max="20" value="14" style="width: 60px; padding: 5px; border: 1px solid #bdbdbd; border-radius: 4px;">
    </div>
    <button id="printButton">Imprimere la conduite </button>
</div>
<script src="print.js"></script>
<link rel="stylesheet" href="print-styles.css">
<script src="certificat-unified-font-size.js"></script>
<script>
document.addEventListener('DOMContentLoaded', () => {
    // Charger la taille de police sauvegardée
    const savedLesionsFontSize = localStorage.getItem('lesionsFontSize') || '14';
    const fontSize1Input = document.getElementById('fontSize1');

    if (fontSize1Input) {
        fontSize1Input.value = savedLesionsFontSize;
        fontSize1Input.addEventListener('input', () => {
            const fontSize = fontSize1Input.value;
            updateFontSizeForLesions(fontSize);
        });
    }

    // Appliquer la taille de police initiale
    updateFontSizeForLesions(savedLesionsFontSize);
});
</script>
</body>
</html>
`;

    const newWindow = window.open("", "_blank");
    newWindow.document.write(certificatContent);
    newWindow.document.close();
}

//cat Leishmaniose plus3

function catLeishmanioseplus3() {
    const nom = document.getElementById('patientNomPrenom').value || '';
    const dob = document.getElementById('patientDateNaissance').value;
    
    const today = new Date();
    const year = today.getFullYear();
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const day = String(today.getDate()).padStart(2, '0');
    const todayFormatted = `${year}-${month}-${day}`;

    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";
    const docteur = localStorage.getItem('docteur') || "";


    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';

    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"></div>';
    }



    const certificatContent = `
<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Cat de Leishmaniose</title>
<style>
body {
font-family: Arial, sans-serif;
padding: 20px;
background-color: #f9f9f9;
}
.certificat {
background-color: white;
border: 1px solid #ddd;
padding: 20px;
max-width: 600px;
margin: 0 auto;
margin-top: 60px;
box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
}
h1 {
text-align: center;
color: #333;
text-decoration: underline;
font-size: 20px;
}
p {
line-height: 1.5;
color: #555;
}
.editable-field {
border-bottom: 1px dashed #666;
display: inline-block;
min-width: 50px;
min-height: 20px;
padding: 2px 4px;
margin: 0 3px;
}
.editable-area {
border: 1px solid #ddd;
border-radius: 4px;
padding: 8px;
margin: 5px 0;
width: 100%;
min-height: 20px;
resize: vertical;
overflow: hidden;
font-family: inherit;
font-size: inherit;
line-height: inherit;
}
.editable-area:focus {
outline: none;
border-color: #007bff;
}
#head {
margin-bottom: 20px;
}
#head table {
width: 100%;
border: 0px solid #000000;
padding: 4px;
margin-bottom: 15px;
}
#head td {
text-align: center;
}
.print-button {
text-align: center;
margin-top: 20px;
}
.print-button button {
padding: 10px 20px;
font-size: 16px;
background-color: #007bff;
color: white;
border: none;
border-radius: 5px;
cursor: pointer;
}
.print-button button:hover {
background-color: #0056b3;
}
@media print {
@page {
size: A5;
margin: 0.2cm 0.2cm 0.2cm 0.2cm;
}
body {
margin: 0 !important;
padding: 0 !important;
font-size: 10pt !important;
background-color: white;
}
.certificat {
padding: 2px 8px !important;
max-width: 100% !important;
border: none;
box-shadow: none;
margin-top: 0;
}
h1 {
font-size: 14pt !important;
margin: 5px 0 !important;
}
p {
font-size: 9pt !important;
margin: 2px 0 !important;
line-height: 1.2 !important;
}
input[type="text"],
input[type="date"],
textarea {
border: none !important;
background: none !important;
box-shadow: none !important;
outline: none !important;
font-size: 9pt !important;
}
input[type="text"]:focus,
input[type="date"]:focus,
textarea:focus {
border: none !important;
outline: none !important;
}
.print-button {
display: none;
}
.editable-field, .editable-area {
border: none !important;
}
.docteur {
font-weight: bold;
font-size: 14pt !important;
margin-right: 50px;
}
}
</style>
</head>
<body>
${enteteContent}
<div class="certificat" style="font-family: Arial, sans-serif; line-height: 1.6;">
<h1 style="text-align: center; color: #2c3e50;">Cher confrère</h1>
<div class="contenu-certificat" style="margin-top: 1.5cm !important;">
<p style="margin: 15px 0;">
Permettez moi de vous adresser le(a) nommé(e) <strong><input type="text" value="${nom}" style="width: 200px;"></strong>  <br>
qui consulte chez nous pour leishmaniose cutanée et qui présente<br>
<input type="text" value="plus de 03 lésions cutanées"
style="width: 100%; padding: 8px; margin: 10px 0; border: 1px solid #ddd; border-radius: 4px;" /><br>
(et selon l'instruction de leishmaniose N06 du 16 oct 2011)<br>
relative à  la catégorie de leishmaniose cutanée.<br><br>

Il(Elle) nécessite la voie IM selon ses fonctions vitales qui nécessitent<br>
l'avis et PEC spécialisée (et meme parfois l'hospitalisation).<br>
Je vous le(la) confie pour une prise en charge adéquate.
</p>
<p style="text-align: right; margin-top: 30px;">
Avec nos remerciements pour votre collaboration,<br>
<span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>&nbsp;
</p>
</div>
<div class="print-button" style="display: flex; align-items: center; justify-content: center; gap: 15px;">
    <div class="print-controls" style="display: flex; align-items: center; gap: 8px;">
        <label for="fontSize2" style="font-size: 14px; margin: 0;">Taille du texte:</label>
        <input type="number" id="fontSize2" min="8" max="20" value="14" style="width: 60px; padding: 5px; border: 1px solid #bdbdbd; border-radius: 4px;">
    </div>
    <button id="printButton">Imprimer la lettre </button>
</div>
<script src="print.js"></script>
<link rel="stylesheet" href="print-styles.css">
<script src="certificat-unified-font-size.js"></script>
<script>
document.addEventListener('DOMContentLoaded', () => {
    // Charger la taille de police sauvegardée
    const savedLesionsFontSize = localStorage.getItem('lesionsFontSize') || '14';
    const fontSize2Input = document.getElementById('fontSize2');

    if (fontSize2Input) {
        fontSize2Input.value = savedLesionsFontSize;
        fontSize2Input.addEventListener('input', () => {
            const fontSize = fontSize2Input.value;
            updateFontSizeForLesions(fontSize);
        });
    }

    // Appliquer la taille de police initiale
    updateFontSizeForLesions(savedLesionsFontSize);
});
</script>
</body>
</html>
`;

    const newWindow = window.open("", "_blank");
    newWindow.document.write(certificatContent);
    newWindow.document.close();
}
function ouvrirCertificatMalVision() {
    // Récupérer les informations du patient depuis les champs du formulaire
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '';
    const patientAge = document.getElementById('patientAge').value || '';
    const patientDateNaissance = document.getElementById('patientDateNaissance').value || '';
    
    // Diviser le nom et prénom
    let nom = '';
    let prenom = '';
    if (patientNomPrenom) {
        const parts = patientNomPrenom.split(' ');
        nom = parts[0] || '';
        prenom = parts.slice(1).join(' ') || '';
    }
    
    // Utiliser la date de naissance si disponible
    const dob = patientDateNaissance || '';

    const today = new Date();
    const year = today.getFullYear();
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const day = String(today.getDate()).padStart(2, '0');
    const todayFormatted = `${year}-${month}-${day}`;

    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";
    const docteur = localStorage.getItem('docteur') || "";

    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';

    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"></div>';
    }

    const certificatContent = `
<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Certificat de Mauvaise Vision</title>
    <style>
body {
font-family: Arial, sans-serif;
padding: 20px;
background-color: #f9f9f9;
}
.certificat {
background-color: white;
border: 1px solid #ddd;
padding: 20px;
max-width: 600px;
margin: 0 auto;
margin-top: 60px;
box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
}
h1 {
text-align: center;
color: #333;
text-decoration: underline;
font-size: 20px;
}
p {
line-height: 1.5;
color: #555;
}
.editable-field {
border-bottom: 1px dashed #666;
display: inline-block;
min-width: 50px;
min-height: 20px;
padding: 2px 4px;
margin: 0 3px;
}
.editable-area {
border: 1px solid #ddd;
border-radius: 4px;
padding: 8px;
margin: 5px 0;
width: 100%;
min-height: 20px;
resize: vertical;
overflow: hidden;
font-family: inherit;
font-size: inherit;
line-height: inherit;
}
.editable-area:focus {
outline: none;
border-color: #007bff;
}
#head {
margin-bottom: 20px;
}
#head table {
width: 100%;
border: 0px solid #000000;
padding: 4px;
margin-bottom: 15px;
}
#head td {
text-align: center;
}
.print-button {
text-align: center;
margin-top: 20px;
}
.print-button button {
padding: 10px 20px;
font-size: 16px;
background-color: #007bff;
color: white;
border: none;
border-radius: 5px;
cursor: pointer;
}
.print-button button:hover {
background-color: #0056b3;
}
@media print {
@page {
    size: A5;
    margin: 0.2cm 0.2cm 0.2cm 0.2cm;
}

body {
    margin: 0 !important;
    padding: 0 !important;
    font-size: 10pt !important;
    line-height: 1.2 !important;
    background-color: white;
}

.certificat {
    border: none;
    box-shadow: none;
    margin: 0 !important;
    padding: 2px 8px !important;
    max-width: 100% !important;
}

h1 {
    font-size: 14pt !important;
    margin: 5px 0 !important;
    margin-top: 2cm !important;
}

input[type="text"],
input[type="date"],
textarea {
    border: none !important;
    background: none !important;
    box-shadow: none !important;
    outline: none !important;
    font-size: 9pt !important;
}

input[type="text"]:focus,
input[type="date"]:focus,
textarea:focus {
    border: none !important;
    outline: none !important;
}

/* Styles existants */
.print-button {
    display: none;
}
.editable-field, .editable-area {
    border: none !important;
}

/* Additional space optimization */
* {
    margin-top: 0 !important;
    margin-bottom: 2px !important;
}

p {
    margin: 2px 0 !important;
    font-size: 9pt !important;
}
}
</style>
</head>
<body>
${enteteContent}
    <div class="certificat">
        <h1>CERTIFICAT MEDICAL</h1>
        <div class="contenu-certificat" style="margin-top: 1.5cm !important;">
        <p>
            Je soussigné, Dr <input type="text" id="docteur" value="${docteur}" placeholder="" style="width: 120px;">,
            certifie avoir examiné <strong><input type="text" value="${nom} ${prenom}" style="width: 180px;"></strong>,
            <span class="editable-field" contenteditable="true" style="min-width: 100px; display: inline-block;">né(e) le ${dob}</span> dont l'examen ce jour retrouve : <span class="editable-field" contenteditable="true" style="min-width: 300px; display: inline-block;">Une mal vision bilatérale nécessitant le port de lunettes</span> et d'être placé aux premiers rangs de la classe.
        </p>
        <p style="text-align: right; margin-top: 30px;">
            Le <span class="editable-field" contenteditable="true" style="min-width: 120px; display: inline-block;">${todayFormatted}</span>
        </p>
        <p style="text-align: right; margin-top: 30px;">
            Dont certificat<br>
            <span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>
        </p>
    </div>
    <div class="print-button" style="display: flex; align-items: center; justify-content: center; gap: 15px;">
        <div style="display: flex; align-items: center; gap: 8px;">
            <label for="fontSize" style="font-size: 14px; margin: 0;">Taille du texte:</label>
            <input type="number" id="fontSize" min="8" max="20" value="14" style="width: 60px; padding: 5px; border: 1px solid #bdbdbd; border-radius: 4px;">
        </div>
        <button id="printButton">Imprimer le Certificat</button>
    </div>
    <script src="print.js"></script>
    <script src="certificat-unified-font-size.js"></script>

</body>
</html>
`;

    var newWindow = window.open("", "_blank");
    if (newWindow) {
        newWindow.document.write(certificatContent);
        newWindow.document.close();

        // Attacher l'événement d'impression directement après la fermeture du document
        newWindow.onload = function () {
            const printButton = newWindow.document.getElementById('printButton');
            if (printButton) {
                printButton.addEventListener('click', function () {
                    newWindow.print();
                });
            }
        };
    } else {
        console.log("Popup bloquée par le navigateur.");
    }
}

// Fonction pour ouvrir une modale
function openModal(content) {
    const modal = document.createElement('div');
    modal.style.position = 'fixed';
    modal.style.top = '0';
    modal.style.left = '0';
    modal.style.width = '100%';
    modal.style.height = '100%';
    modal.style.backgroundColor = 'rgba(0, 0, 0, 0.7)';
    modal.style.display = 'flex';
    modal.style.justifyContent = 'center';
    modal.style.alignItems = 'center';
    modal.innerHTML = content;

    document.body.appendChild(modal);

    // Ajout des écouteurs pour les boutons
    // (Vous pouvez ajouter ici les écouteurs pour les autres boutons dans cette modal)

    // Fermer la modale si l'utilisateur clique en dehors
    modal.addEventListener('click', (event) => {
        if (event.target === modal) {
            document.body.removeChild(modal);
        }
    });
}

// Fonction pour demander le poids du patient
function demanderPoids() {
    const today = new Date();
    const year = today.getFullYear();
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const day = String(today.getDate()).padStart(2, '0');
    const todayFormatted = `${year}-${month}-${day}`;

    const modalContent = `
                <div style="background: white; padding: 20px; border-radius: 5px; text-align: center;">
                    <h3>Poids du Patient</h3>
                    <p>Quel est le poids de votre patient en kg ?</p>
                    <input type="number" id="poidsPatient" style="padding: 8px; margin: 10px 0;">
                    <p>Date de la morsure :</p>
                    <input type="date" id="dateMorsure" value="${todayFormatted}" style="padding: 8px; margin: 10px 0;">
                    <div>
                        <button id="confirmPoids" style="padding: 8px 16px; background-color: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">Confirmer</button>
                    </div>
                </div>`;

    const modal = document.createElement('div');
    modal.style.position = 'fixed';
    modal.style.top = '0';
    modal.style.left = '0';
    modal.style.width = '100%';
    modal.style.height = '100%';
    modal.style.backgroundColor = 'rgba(0, 0, 0, 0.7)';
    modal.style.display = 'flex';
    modal.style.justifyContent = 'center';
    modal.style.alignItems = 'center';
    modal.style.zIndex = '1000';
    modal.innerHTML = modalContent;

    document.body.appendChild(modal);

    modal.querySelector('#confirmPoids').addEventListener('click', function () {
        const poidsInput = modal.querySelector('#poidsPatient').value;
        const dateMorsure = modal.querySelector('#dateMorsure').value;

        if (!poidsInput || !dateMorsure) {
            alert("Veuillez remplir tous les champs.(valeur numérique dans la case de poids)");
            return;
        }

        document.body.removeChild(modal);
        // Ouvrir la modale de choix du schéma pour Vaccin C (sans Tissulaire avec SAR)
        ouvrirModalChoixSchemaVaccinC(dateMorsure, poidsInput);
    });

    modal.addEventListener('click', function (e) {
        if (e.target === modal) {
            document.body.removeChild(modal);
        }
    });
}

// Fonction pour demander le poids du patient pour le vaccin T
function demanderPoidsT() {
    const today = new Date();
    const year = today.getFullYear();
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const day = String(today.getDate()).padStart(2, '0');
    const todayFormatted = `${year}-${month}-${day}`;

    const modalContent = `
                <div style="background: white; padding: 20px; border-radius: 5px; text-align: center;">
                    <h3>Poids du Patient</h3>
                    <p>Quel est le poids de votre patient en kg ?</p>
                    <input type="number" id="poidsPatient" style="padding: 8px; margin: 10px 0;">
                    <p>Date de la morsure :</p>
                    <input type="date" id="dateMorsure" value="${todayFormatted}" style="padding: 8px; margin: 10px 0;">
                    <div>
                        <button id="confirmPoids" style="padding: 8px 16px; background-color: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">Confirmer</button>
                    </div>
                </div>`;

    const modal = document.createElement('div');
    modal.style.position = 'fixed';
    modal.style.top = '0';
    modal.style.left = '0';
    modal.style.width = '100%';
    modal.style.height = '100%';
    modal.style.backgroundColor = 'rgba(0, 0, 0, 0.7)';
    modal.style.display = 'flex';
    modal.style.justifyContent = 'center';
    modal.style.alignItems = 'center';
    modal.style.zIndex = '1000';
    modal.innerHTML = modalContent;

    document.body.appendChild(modal);

    modal.querySelector('#confirmPoids').addEventListener('click', function () {
        const poidsInput = modal.querySelector('#poidsPatient').value;
        const dateMorsure = modal.querySelector('#dateMorsure').value;

        if (!poidsInput || !dateMorsure) {
            alert("Veuillez remplir tous les champs.(valeur numérique dans la case de poids)");
            return;
        }

        document.body.removeChild(modal);
        // Appeler directement le certificat Tissulaire avec SAR
        vaccint3(dateMorsure, poidsInput);
    });

    modal.addEventListener('click', function (e) {
        if (e.target === modal) {
            document.body.removeChild(modal);
        }
    });
}

// Fonction pour demander la date pour les patients immunocompétents (sans poids)
function demanderDateImmunocompetent() {
    const today = new Date();
    const year = today.getFullYear();
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const day = String(today.getDate()).padStart(2, '0');
    const todayFormatted = `${year}-${month}-${day}`;

    const modalContent = `
        <div style="background: white; padding: 20px; border-radius: 5px; text-align: center;">
            <h3>Prophylaxie Pré-exposition - Patient Immunocompétent</h3>
            <p>Date de début de la prophylaxie :</p>
            <input type="date" id="dateDebut" value="${todayFormatted}" style="padding: 8px; margin: 10px 0;">
            <div>
                <button id="confirmDate" style="padding: 8px 16px; background-color: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">Confirmer</button>
            </div>
        </div>`;

    const modal = document.createElement('div');
    modal.style.position = 'fixed';
    modal.style.top = '0';
    modal.style.left = '0';
    modal.style.width = '100%';
    modal.style.height = '100%';
    modal.style.backgroundColor = 'rgba(0, 0, 0, 0.7)';
    modal.style.display = 'flex';
    modal.style.justifyContent = 'center';
    modal.style.alignItems = 'center';
    modal.style.zIndex = '1000';
    modal.innerHTML = modalContent;

    document.body.appendChild(modal);

    modal.querySelector('#confirmDate').addEventListener('click', function () {
        const dateDebut = modal.querySelector('#dateDebut').value;

        if (!dateDebut) {
            alert("Veuillez sélectionner une date.");
            return;
        }

        document.body.removeChild(modal);
        // Appeler la fonction pour générer le certificat de prophylaxie pré-exposition
        genererCertificatProphylaxieImmunocompetent(dateDebut);
    });

    modal.addEventListener('click', function (e) {
        if (e.target === modal) {
            document.body.removeChild(modal);
        }
    });
}

// Fonction pour demander la date pour les patients immunodéprimés
function demanderDateImmunoDeprime() {
    const today = new Date();
    const year = today.getFullYear();
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const day = String(today.getDate()).padStart(2, '0');
    const todayFormatted = `${year}-${month}-${day}`;

    const modalContent = `
        <div style="background: white; padding: 20px; border-radius: 5px; text-align: center;">
            <h3>Prophylaxie Pré-exposition - Patient Immunodéprimé</h3>
            <p>Date de début de la prophylaxie :</p>
            <input type="date" id="dateDebut" value="${todayFormatted}" style="padding: 8px; margin: 10px 0;">
            <div>
                <button id="confirmDate" style="padding: 8px 16px; background-color: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">Confirmer</button>
            </div>
        </div>`;

    const modal = document.createElement('div');
    modal.style.position = 'fixed';
    modal.style.top = '0';
    modal.style.left = '0';
    modal.style.width = '100%';
    modal.style.height = '100%';
    modal.style.backgroundColor = 'rgba(0, 0, 0, 0.7)';
    modal.style.display = 'flex';
    modal.style.justifyContent = 'center';
    modal.style.alignItems = 'center';
    modal.style.zIndex = '1000';
    modal.innerHTML = modalContent;

    document.body.appendChild(modal);

    modal.querySelector('#confirmDate').addEventListener('click', function () {
        const dateDebut = modal.querySelector('#dateDebut').value;

        if (!dateDebut) {
            alert("Veuillez sélectionner une date.");
            return;
        }

        document.body.removeChild(modal);
        // Appeler la fonction pour générer le certificat de prophylaxie pré-exposition pour immunodéprimé
        genererCertificatProphylaxieImmunoDeprime(dateDebut);
    });

    modal.addEventListener('click', function (e) {
        if (e.target === modal) {
            document.body.removeChild(modal);
        }
    });
}

// Fonction pour demander la date et le poids pour ATCD Prophylaxie Pré-exposition
function demanderDateATCDProphylaxie() {
    const today = new Date();
    const year = today.getFullYear();
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const day = String(today.getDate()).padStart(2, '0');
    const todayFormatted = `${year}-${month}-${day}`;

    const modalContent = `
        <div style="background: white; padding: 20px; border-radius: 5px; text-align: center;">
            <h3>Avec ATCD Prophylaxie Pré-exposition</h3>
            <p>Date de la morsure :</p>
            <input type="date" id="dateMorsure" value="${todayFormatted}" style="padding: 8px; margin: 10px 0;">
            <div>
                <button id="confirmDate" style="padding: 8px 16px; background-color: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">Confirmer</button>
            </div>
        </div>`;

    const modal = document.createElement('div');
    modal.style.position = 'fixed';
    modal.style.top = '0';
    modal.style.left = '0';
    modal.style.width = '100%';
    modal.style.height = '100%';
    modal.style.backgroundColor = 'rgba(0, 0, 0, 0.7)';
    modal.style.display = 'flex';
    modal.style.justifyContent = 'center';
    modal.style.alignItems = 'center';
    modal.style.zIndex = '1000';
    modal.innerHTML = modalContent;

    document.body.appendChild(modal);

    modal.querySelector('#confirmDate').addEventListener('click', function () {
        const dateMorsure = modal.querySelector('#dateMorsure').value;

        if (!dateMorsure) {
            alert("Veuillez sélectionner une date.");
            return;
        }

        document.body.removeChild(modal);
        // Appeler la fonction prophylaxiePreExpositionSchema3 avec seulement la date
        prophylaxiePreExpositionSchema3(dateMorsure);
    });

    modal.addEventListener('click', function (e) {
        if (e.target === modal) {
            document.body.removeChild(modal);
        }
    });
}


// Fonction pour ouvrir la modale pour Classe 02
function ouvrirModalClasse02() {
    const modalContent = `
        <div>
            <h3 style="color: green;">Choisissez un vaccin :</h3>
            <button id="vaccinCellulaire">Vaccin Cellulaire</button>
            <button id="vaccinTissulaire">Vaccin Tissulaire</button>
        </div>
    `;
    openModal(modalContent);

    // Ecouteur pour le bouton Vaccin Cellulaire
    document.querySelector('#vaccinCellulaire').addEventListener('click', () => {
        // Demander uniquement la date (le poids n'est pas nécessaire pour la classe 02)
        demanderDatePourClasse02();
    });

    // Ecouteur pour le bouton Vaccin Tissulaire
    document.querySelector('#vaccinTissulaire').addEventListener('click', () => {
        // Demander uniquement la date (le poids n'est pas nécessaire pour la classe 02)
        demanderDatePourClasse02Tissulaire();
    });
}

// Fonction pour ouvrir la modale pour Classe 03
function ouvrirModalClasse03() {
    const modalContent = `
<div>
<h3 style="color: green;">Choisissez un vaccin :</h3>
<button id="vaccinC">Vaccin C (Cellulaire)</button>
<button id="vaccinT">Vaccin T (Tissulaire)</button>
</div>
`;
    openModal(modalContent);
    // Ecouteur pour le bouton Vaccin C
    document.querySelector('#vaccinC').addEventListener('click', () => {
        demanderPoids(); // Demande le poids du patient
    });

    // Ecouteur pour le bouton Vaccin T
    document.querySelector('#vaccinT').addEventListener('click', () => {
        demanderPoidsT(); // Appelle la fonction qui ouvre la modale de choix de schéma
    });
}

// Fonction pour ouvrir la modale pour Prophylaxie Pré-exposition
function ouvrirModalPrex() {
    const modalContent = `
<div>
<h3 style="color: green;">Choisissez le type de patient :</h3>
<button id="immunocompetent">01 Immunocompétent</button>
<button id="immunodeprime">02 Immunodéprimé</button>
<button id="prophylaxiePreExpositionSchema3" style="padding: 8px 16px; margin: 5px; background-color: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">03 Avec Risque Hemorragique</button>
</div>
`;
    openModal(modalContent);
// Écouteur pour le bouton Immunocompétent
    document.querySelector('#immunocompetent').addEventListener('click', () => {
        // Demander uniquement la date (pas de poids nécessaire pour prophylaxie pré-exposition)
        demanderDateImmunocompetent();
    });

// Écouteur pour le bouton Immunodéprimé
    document.querySelector('#immunodeprime').addEventListener('click', () => {
        // Demander uniquement la date (pas de poids nécessaire pour prophylaxie pré-exposition)
        demanderDateImmunoDeprime();
    });

// Écouteur pour le bouton Avec ATCD Prophylaxie Pré-exposition
    document.querySelector('#prophylaxiePreExpositionSchema3').addEventListener('click', () => {
        // Demander uniquement la date (pas de poids nécessaire pour prophylaxie pré-exposition)
        demanderDateATCDProphylaxie();
    });
}

// Fonction pour demander la date pour la classe 02 (vaccin cellulaire)
function demanderDatePourClasse02() {
    const today = new Date();
    const year = today.getFullYear();
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const day = String(today.getDate()).padStart(2, '0');
    const todayFormatted = `${year}-${month}-${day}`;

    const modalContent = `
                <div style="background: white; padding: 20px; border-radius: 5px; text-align: center;">
                    <h3>Date de la morsure</h3>
                    <p>Veuillez entrer la date du 01er jour de vaccination (Jour 0) :</p>
                    <input type="date" id="dateMorsure" value="${todayFormatted}" style="padding: 8px; margin: 10px 0;">
                    <div>
                        <button id="confirmDate" style="padding: 8px 16px; background-color: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">Confirmer</button>
                    </div>
                </div>`;

    const modal = document.createElement('div');
    modal.style.position = 'fixed';
    modal.style.top = '0';
    modal.style.left = '0';
    modal.style.width = '100%';
    modal.style.height = '100%';
    modal.style.backgroundColor = 'rgba(0, 0, 0, 0.7)';
    modal.style.display = 'flex';
    modal.style.justifyContent = 'center';
    modal.style.alignItems = 'center';
    modal.style.zIndex = '1000';
    modal.innerHTML = modalContent;

    document.body.appendChild(modal);

    modal.querySelector('#confirmDate').addEventListener('click', function () {
        const dateMorsure = modal.querySelector('#dateMorsure').value;

        if (!dateMorsure) {
            alert("Veuillez entrer une date valide.");
            return;
        }

        document.body.removeChild(modal);
        // Ouvrir la modale de choix du schéma pour Vaccin Cellulaire
        openVaccinChoiceModal(dateMorsure, '');
    });

    modal.addEventListener('click', function (e) {
        if (e.target === modal) {
            document.body.removeChild(modal);
        }
    });
}

// Fonction pour demander la date pour la classe 02 (vaccin tissulaire)
function demanderDatePourClasse02Tissulaire() {
    const today = new Date();
    const year = today.getFullYear();
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const day = String(today.getDate()).padStart(2, '0');
    const todayFormatted = `${year}-${month}-${day}`;

    const modalContent = `
                <div style="background: white; padding: 20px; border-radius: 5px; text-align: center;">
                    <h3>Date de la morsure</h3>
                    <p>Veuillez entrer la date du 01er jour de vaccination (Jour 0) :</p>
                    <input type="date" id="dateMorsure" value="${todayFormatted}" style="padding: 8px; margin: 10px 0;">
                    <div>
                        <button id="confirmDate" style="padding: 8px 16px; background-color: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">Confirmer</button>
                    </div>
                </div>`;

    const modal = document.createElement('div');
    modal.style.position = 'fixed';
    modal.style.top = '0';
    modal.style.left = '0';
    modal.style.width = '100%';
    modal.style.height = '100%';
    modal.style.backgroundColor = 'rgba(0, 0, 0, 0.7)';
    modal.style.display = 'flex';
    modal.style.justifyContent = 'center';
    modal.style.alignItems = 'center';
    modal.style.zIndex = '1000';
    modal.innerHTML = modalContent;

    document.body.appendChild(modal);

    modal.querySelector('#confirmDate').addEventListener('click', function () {
        const dateMorsure = modal.querySelector('#dateMorsure').value;

        if (!dateMorsure) {
            alert("Veuillez entrer une date valide.");
            return;
        }

        document.body.removeChild(modal);
        // Appeler directement la fonction Tissulairesanssar sans le poids
        Tissulairesanssar(dateMorsure, '');
    });

    modal.addEventListener('click', function (e) {
        if (e.target === modal) {
            document.body.removeChild(modal);
        }
    });
}

// Fonction pour ouvrir une modale pour le choix entre Zagreb et Essens
function openVaccinChoiceModal(dateMorsure, poidsInput) {
    const modalContent = `
        <div>
            <h3 style="color: green;">Choisissez un schéma :</h3>
            <button id="zegreb">Zagreb</button>
            <button id="essens">Essen</button>
            <button id="risqueHemorragique" style="padding: 8px 16px; margin: 5px; background-color: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">Risque hémorragique/qte limitées</button>
            <button id="prophylaxiePreExpositionSchema1Classe2" style="padding: 8px 16px; margin: 5px; background-color: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">Avec ATCD Vaccinaux (Schéma 1)</button>
            <button id="prophylaxiePreExpositionSchema2Classe2" style="padding: 8px 16px; margin: 5px; background-color: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">Avec ATCD Vaccinaux (Schéma 2)</button>
        </div>
    `;

    const modal = document.createElement('div');
    modal.style.position = 'fixed';
    modal.style.top = '0';
    modal.style.left = '0';
    modal.style.width = '100%';
    modal.style.height = '100%';
    modal.style.backgroundColor = 'rgba(0, 0, 0, 0.7)';
    modal.style.display = 'flex';
    modal.style.justifyContent = 'center';
    modal.style.alignItems = 'center';
    modal.innerHTML = modalContent;

    document.body.appendChild(modal);

    // Ecouteurs pour les boutons dans la modale
    modal.querySelector('#zegreb').addEventListener('click', () => {
        zegreb(dateMorsure, poidsInput); // Pass the date and weight
        document.body.removeChild(modal);
    });

    modal.querySelector('#essens').addEventListener('click', () => {
        essens(dateMorsure, poidsInput); // Pass the date and weight
        document.body.removeChild(modal);
    });

    // Removed event listener for #avecATCDVaccinauxIM as this button was removed per user request

    modal.querySelector('#risqueHemorragique').addEventListener('click', () => {
        risqueHemorragiqueClasse2(dateMorsure, poidsInput); // Pass the date and weight
        document.body.removeChild(modal);
    });

    modal.querySelector('#prophylaxiePreExpositionSchema1Classe2').addEventListener('click', () => {
        prophylaxiePreExpositionSchema1Classe2(dateMorsure, poidsInput); // Pass the date and weight
        document.body.removeChild(modal);
    });

    modal.querySelector('#prophylaxiePreExpositionSchema2Classe2').addEventListener('click', () => {
        prophylaxiePreExpositionSchema2Classe2(dateMorsure); // Only pass the date, no weight needed
        document.body.removeChild(modal);
    });

    // Fermer la modale si l'utilisateur clique en dehors
    modal.addEventListener('click', (event) => {
        // Vérifier si le clic est sur la modal elle-même (et non sur un élément enfant)
        if (event.target === modal) {
            document.body.removeChild(modal);
        }
    });
}

// Nouvelle fonction pour ouvrir la modale de choix de schéma pour Vaccin C uniquement (sans Tissulaire avec SAR)
function ouvrirModalChoixSchemaVaccinC(dateMorsure, poidsInput) {
    const modalContent = `
    <div style="background: white; padding: 20px; border-radius: 5px; text-align: center;">
        <h3 style="color: green;">Choisissez un schéma :</h3>
        <button id="zegreb3" style="padding: 8px 16px; margin: 5px; background-color: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">Zagreb3</button>
        <button id="essen3" style="padding: 8px 16px; margin: 5px; background-color: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">Essen3</button>
        <button id="risqueHemorragique3" style="padding: 8px 16px; margin: 5px; background-color: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">Risque Hémorragique/Qte Limitées</button>
        <button id="prophylaxiePreExpositionSchema1Classe3" style="padding: 8px 16px; margin: 5px; background-color: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">Avec ATCD Vaccinaux (Schéma 1)</button>
        <button id="prophylaxiePreExpositionSchema2Classe3" style="padding: 8px 16px; margin: 5px; background-color: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">Avec ATCD Vaccinaux (Schéma 2)</button>
    </div>`;

    const modal = document.createElement('div');
    modal.style.position = 'fixed';
    modal.style.top = '0';
    modal.style.left = '0';
    modal.style.width = '100%';
    modal.style.height = '100%';
    modal.style.backgroundColor = 'rgba(0, 0, 0, 0.7)';
    modal.style.display = 'flex';
    modal.style.justifyContent = 'center';
    modal.style.alignItems = 'center';
    modal.style.zIndex = '1000';
    modal.innerHTML = modalContent;

    document.body.appendChild(modal);

    // Ecouteur pour le bouton Zagreb3
    modal.querySelector('#zegreb3').addEventListener('click', () => {
        document.body.removeChild(modal);
        vaccinc3(dateMorsure, poidsInput);
    });

    // Ecouteur pour le bouton Essen3
    modal.querySelector('#essen3').addEventListener('click', () => {
        document.body.removeChild(modal);
        essen3(dateMorsure, poidsInput);
    });

    // Ecouteur pour le bouton Risque Hémorragique/Qte Limitées
    modal.querySelector('#risqueHemorragique3').addEventListener('click', () => {
        document.body.removeChild(modal);
        risqueHemorragique3(dateMorsure, poidsInput);
    });

    // Ecouteur pour le bouton Avec ATCD Vaccinaux (Schéma 1)
    modal.querySelector('#prophylaxiePreExpositionSchema1Classe3').addEventListener('click', () => {
        document.body.removeChild(modal);
        prophylaxiePreExpositionSchema1Classe3(dateMorsure, poidsInput);
    });

    // Ecouteur pour le bouton  Avec ATCD Vaccinaux (Schéma 2)
    modal.querySelector('#prophylaxiePreExpositionSchema2Classe3').addEventListener('click', () => {
        document.body.removeChild(modal);
        prophylaxiePreExpositionSchema2Classe3(dateMorsure, poidsInput);
    });

    // Fermer la modale si l'utilisateur clique en dehors
    modal.addEventListener('click', function (e) {
        if (e.target === modal) {
            document.body.removeChild(modal);
        }
    });
}

// Fonctions de schéma de vaccination (placeholders)
// Ces fonctions seront appelées avec dateMorsure et poidsInput comme paramètres
function zegreb(dateMorsure, poidsInput) {
    // Récupérer les informations du patient depuis le formulaire
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '';
    const patientAge = document.getElementById('patientAge').value || '';
    const patientDateNaissance = document.getElementById('patientDateNaissance').value || '';
    
    // Sauvegarder les informations du patient dans le localStorage pour la sauvegarde
    localStorage.setItem('patientNomPrenom', patientNomPrenom);
    localStorage.setItem('patientAge', patientAge);
    localStorage.setItem('patientDateNaissance', patientDateNaissance);
    
    // Utiliser la date de naissance si disponible, sinon l'âge
    let ageInfo = '';
    if (patientDateNaissance) {
        ageInfo = patientDateNaissance;
    } else if (patientAge) {
        ageInfo = patientAge;
    } else {
        ageInfo = '[Date de naissance]';
    }
    
    // Calculer les dates pour le schéma Zagreb (J0, J7, J21)
    const dateJour0 = new Date(dateMorsure);
    const datePlus7 = new Date(dateJour0);
    datePlus7.setDate(dateJour0.getDate() + 7);
    const datePlus21 = new Date(dateJour0);
    datePlus21.setDate(dateJour0.getDate() + 21);

    const dateFormattedJour0 = `${dateJour0.getFullYear()}-${String(dateJour0.getMonth() + 1).padStart(2, '0')}-${String(dateJour0.getDate()).padStart(2, '0')}`;
    const dateFormattedPlus7 = `${datePlus7.getFullYear()}-${String(datePlus7.getMonth() + 1).padStart(2, '0')}-${String(datePlus7.getDate()).padStart(2, '0')}`;
    const dateFormattedPlus21 = `${datePlus21.getFullYear()}-${String(datePlus21.getMonth() + 1).padStart(2, '0')}-${String(datePlus21.getDate()).padStart(2, '0')}`;

    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";
    const docteur = localStorage.getItem('docteur') || "";
    const animal = "chien"; // Animal en cause (valeur par défaut)

    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';

    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"></div>';
    }

    // Générer le certificat Zagreb
    const certificatContent = `
    <!DOCTYPE html>
    <html lang="fr">
    <head>
    <meta charset="UTF-8">
    <title>Certificat Antirabique - Schéma Zagreb</title>
    <style>
    body {
        font-family: Arial, sans-serif;
        padding: 20px;
        background-color: #f9f9f9;
    }
    .certificat {
        background-color: white;
        border: 1px solid #ddd;
        padding: 20px;
        max-width: 600px;
        margin: 0 auto;
        margin-top: 60px;
        box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
    }
    h1 {
        text-align: center;
        color: #333;
        text-decoration: underline;
        font-size: 16px;
    }
    h2 {
        text-align: center;
        color: #555;
        font-size: 10px;
        margin-top: 5px;
        margin-bottom: 15px;
    }
    p {
        line-height: 1.5;
        color: #555;
    }
    .small-text {
        font-size: 12px;
    }
    .print-button {
        text-align: center;
        margin-top: 20px;
    }
    .print-button button {
        padding: 10px 20px;
        font-size: 16px;
        background-color: #007bff;
        color: white;
        border: none;
        border-radius: 5px;
        cursor: pointer;
    }
    .print-button button:hover {
        background-color: #0056b3;
    }
    @media print {
        @page {
            size: A5;
            margin: 0.2cm 0.2cm 0.2cm 0.2cm;
        }
        body {
            margin: 0 !important;
            padding: 0 !important;
            font-size: 10pt !important;
            background-color: white;
        }
        .certificat {
            padding: 2px 8px !important;
            max-width: 100% !important;
            border: none;
            box-shadow: none;
            margin-top: 0;
        }
        h1 {
            font-size: 14pt !important;
            margin: 5px 0 !important;
            margin-top: 2cm !important;
        }
        h2 {
            font-size: 12pt !important;
            margin: 3px 0 !important;
        }
        p {
            font-size: 9pt !important;
            margin: 2px 0 !important;
            line-height: 1.2 !important;
        }
        input[type="text"],
        input[type="date"],
        textarea {
            border: none !important;
            background: none !important;
            box-shadow: none !important;
            outline: none !important;
            font-size: 9pt !important;
        }
        input[type="text"]:focus,
        input[type="date"]:focus,
        textarea:focus {
            border: none !important;
            outline: none !important;
        }
        .print-button {
            display: none;
        }
        .docteur {
            font-weight: bold;
            font-size: 14pt !important;
            margin-right: 50px;
        }
        /* Additional space optimization */
        * {
            margin-top: 0 !important;
            margin-bottom: 2px !important;
        }
    }
    </style>
    </head>
    <body>
   ${enteteContent}

    <div class="certificat">
        <h1>Schéma Antirabique - Classe 02 (Zagreb)</h1>
        <h2>Selon l'instruction N16 du 15 Juillet 2024 relative à la conduite à tenir devant un risque rabique</h2>
        <p>
        NOM : <strong><input type="text" value="${patientNomPrenom}" style="width: auto;"></strong><br>
        Date de naissance : <strong><input type="text" value="${ageInfo}" style="width: auto;"></strong><br>
        Animal en cause : <strong><input type="text" value="${animal}" style="width: auto;"></strong><br>
        </p>
		<br><br>
      Classe 02, schéma choisi : vaccin cellulaire / schéma de Zagreb / sans SAR
       <br><br><br>
        <p>
         <br>
         <br><br>
         Jour 0 : <input type="date" id="dateJour0" value="${dateFormattedJour0}" readonly> ( 02 doses chacune dans un deltoïde ) <br>
         Jour 7 : <input type="date" id="datePlus7" value="${dateFormattedPlus7}" readonly> <br>
         Jour 21 : <input type="date" id="datePlus21" value="${dateFormattedPlus21}" readonly> <br>
         <br><br><br>
         <br><br>
           en cas d'âge <02 ans Face antérolatéral externe de la cuisse droite et gauche<br>
        </p>
        <p style="text-align: right; margin-top: 30px;">
        Medecin traitant <br>
        <span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>&nbsp&nbsp&nbsp&nbsp&nbsp;
        </p>
    </div>

    <div class="print-button">
        <button id="printButton">Imprimer le schéma</button>
		<button id="saveButtonZegr2">Sauvegarder</button>
    </div>
    

    <script>
    function sauvegarderCertificatAntirabique() {
        // Récupérer les informations du patient depuis les champs du certificat affiché
        const inputs = document.querySelectorAll('input[type="text"]');
        const patientNomPrenom = inputs[0] ? inputs[0].value : ''; // Premier input = NOM
        const patientDateNaissance = inputs[1] ? inputs[1].value : ''; // Deuxième input = Date de naissance
        const patientAnimal = inputs[2] ? inputs[2].value : 'chien'; // Troisième input = Animal
        const docteurSpan = document.querySelector('.docteur');
        const docteur = docteurSpan ? docteurSpan.textContent.replace('Dr ', '').trim() : '';
        
        // Préparer les données pour l'API
        const maintenant = new Date();
        const data = {
            nom: patientNomPrenom.split(' ').slice(0, -1).join(' ').trim() || patientNomPrenom, // Extraire le nom
            prenom: patientNomPrenom.split(' ').slice(-1).join(' ').trim() || '', // Extraire le prénom
            medecin: docteur,
            classe: "classe02",
            type_de_vaccin: "Cellulaire",
            shema: "Zagreb",
            date_de_certificat: maintenant.toISOString().split('T')[0], // Date d'aujourd'hui
            date_de_naissance: patientDateNaissance,
            animal: patientAnimal,
            heure_creation: maintenant.toTimeString().split(' ')[0] // Ajouter l'heure pour éviter les doublons
        };
        
        // Envoyer les données à l'API
        fetch('http://localhost:5000/api/ajouter_antirabique', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(data)
        })
        .then(response => response.json())
        .then(result => {
            if (result.success) {
                alert('Certificat antirabique sauvegardé avec succès!');
            } else {
                alert('Erreur lors de la sauvegarde: ' + result.message);
            }
        })
        .catch(error => {
            console.error('Erreur:', error);
            alert('Erreur de connexion au serveur');
        });
    }

    const polycliniqueInput = document.getElementById('polyclinique');
    if (polycliniqueInput) {
        polycliniqueInput.addEventListener('input', function () {
            localStorage.setItem('polyclinique', this.value);
        });
    }

    const polycliniqueArInput = document.getElementById('polyclinique-ar');
    if (polycliniqueArInput) {
        polycliniqueArInput.addEventListener('input', function () {
            localStorage.setItem('polyclinique-ar', this.value);
        });
    }

    document.getElementById('printButton').addEventListener('click', function () {
        window.print();
    });
    
    document.getElementById('saveButtonZegr2').addEventListener('click', function () {
        sauvegarderCertificatAntirabique();
    });
    </script>
    </body>
    </html>
    `;

    var newWindow = window.open("", "_blank");
    if (newWindow) {
        newWindow.document.write(certificatContent);
        newWindow.document.close();
    } else {
        console.log("Popup bloquée par le navigateur.");
    }
}

function essens(dateMorsure, poidsInput) {
    // Récupérer les informations du patient depuis le formulaire
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '';
    const patientAge = document.getElementById('patientAge').value || '';
    const patientDateNaissance = document.getElementById('patientDateNaissance').value || '';
    
    // Sauvegarder les informations du patient dans le localStorage pour la sauvegarde
    localStorage.setItem('patientNomPrenom', patientNomPrenom);
    localStorage.setItem('patientAge', patientAge);
    localStorage.setItem('patientDateNaissance', patientDateNaissance);
    
    // Utiliser la date de naissance si disponible, sinon l'âge
    let ageInfo = '';
    if (patientDateNaissance) {
        ageInfo = patientDateNaissance;
    } else if (patientAge) {
        ageInfo = patientAge;
    } else {
        ageInfo = '[Date de naissance]';
    }
    
    // Calculer les dates pour le schéma Essen (J0, J3, J7, J14)
    const dateJour0 = new Date(dateMorsure);
    const datePlus3 = new Date(dateJour0);
    datePlus3.setDate(dateJour0.getDate() + 3);
    const datePlus7 = new Date(dateJour0);
    datePlus7.setDate(dateJour0.getDate() + 7);
    const datePlus14 = new Date(dateJour0);
    datePlus14.setDate(dateJour0.getDate() + 14);

    const dateFormattedJour0 = `${dateJour0.getFullYear()}-${String(dateJour0.getMonth() + 1).padStart(2, '0')}-${String(dateJour0.getDate()).padStart(2, '0')}`;
    const dateFormattedPlus3 = `${datePlus3.getFullYear()}-${String(datePlus3.getMonth() + 1).padStart(2, '0')}-${String(datePlus3.getDate()).padStart(2, '0')}`;
    const dateFormattedPlus7 = `${datePlus7.getFullYear()}-${String(datePlus7.getMonth() + 1).padStart(2, '0')}-${String(datePlus7.getDate()).padStart(2, '0')}`;
    const dateFormattedPlus14 = `${datePlus14.getFullYear()}-${String(datePlus14.getMonth() + 1).padStart(2, '0')}-${String(datePlus14.getDate()).padStart(2, '0')}`;

    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";
    const docteur = localStorage.getItem('docteur') || "";
    const animal = "chien"; // Animal en cause (valeur par défaut)

    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';

    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"></div>';
    }

    // Générer le certificat Essen
    const certificatContent = `
    <!DOCTYPE html>
    <html lang="fr">
    <head>
    <meta charset="UTF-8">
    <title>Certificat Antirabique - Schéma Essen</title>
    <style>
    body {
        font-family: Arial, sans-serif;
        padding: 20px;
        background-color: #f9f9f9;
    }
    .certificat {
        background-color: white;
        border: 1px solid #ddd;
        padding: 20px;
        max-width: 600px;
        margin: 0 auto;
        margin-top: 60px;
        box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
    }
    h1 {
        text-align: center;
        color: #333;
        text-decoration: underline;
        font-size: 16px;
    }
    h2 {
        text-align: center;
        color: #555;
        font-size: 10px;
        margin-top: 5px;
        margin-bottom: 15px;
    }
    p {
        line-height: 1.5;
        color: #555;
    }
    .small-text {
        font-size: 12px;
    }
    .print-button {
        text-align: center;
        margin-top: 20px;
    }
    .print-button button {
        padding: 10px 20px;
        font-size: 16px;
        background-color: #007bff;
        color: white;
        border: none;
        border-radius: 5px;
        cursor: pointer;
    }
    .print-button button:hover {
        background-color: #0056b3;
    }
    @media print {
        @page {
            size: A5;
            margin: 0.2cm 0.2cm 0.2cm 0.2cm;
        }
        body {
            margin: 0 !important;
            padding: 0 !important;
            font-size: 10pt !important;
            background-color: white;
        }
        .certificat {
            padding: 2px 8px !important;
            max-width: 100% !important;
            border: none;
            box-shadow: none;
            margin-top: 0;
        }
        h1 {
            font-size: 14pt !important;
            margin: 5px 0 !important;
            margin-top: 2cm !important;
        }
        h2 {
            font-size: 12pt !important;
            margin: 3px 0 !important;
        }
        p {
            font-size: 9pt !important;
            margin: 2px 0 !important;
            line-height: 1.2 !important;
        }
        input[type="text"],
        input[type="date"],
        textarea {
            border: none !important;
            background: none !important;
            box-shadow: none !important;
            outline: none !important;
            font-size: 9pt !important;
        }
        input[type="text"]:focus,
        input[type="date"]:focus,
        textarea:focus {
            border: none !important;
            outline: none !important;
        }
        .print-button {
            display: none;
        }
        .docteur {
            font-weight: bold;
            font-size: 14pt !important;
            margin-right: 50px;
        }
        /* Additional space optimization */
        * {
            margin-top: 0 !important;
            margin-bottom: 2px !important;
        }
    }
    </style>
    </head>
    <body>
   ${enteteContent}

    <div class="certificat">
        <h1>Schéma Antirabique - Classe 02 (Essen)</h1>
        <h2>Selon l'instruction N16 du 15 Juillet 2024 relative à la conduite à tenir devant un risque rabique</h2>
        <p>
        NOM : <strong><input type="text" value="${patientNomPrenom}" style="width: auto;"></strong><br>
        Date de naissance : <strong><input type="text" value="${ageInfo}" style="width: auto;"></strong><br>
        Animal en cause : <strong><input type="text" value="${animal}" style="width: auto;"></strong><br>
		<br><br>
      Classe 02, schéma choisi : vaccin cellulaire / schéma de Essen / sans SAR
      <br><br><br>
        </p>
        <p>
         <br>
         <br><br>
         Jour 0 : <input type="date" id="dateJour0" value="${dateFormattedJour0}" readonly> ( dans le deltoïde )<br>
         Jour 3 : <input type="date" id="datePlus7" value="${dateFormattedPlus3}" readonly> <br>
         Jour 7 : <input type="date" id="datePlus21" value="${dateFormattedPlus7}" readonly><br>
         Jour 14 : <input type="date" id="datePlus28" value="${dateFormattedPlus14}" readonly> <br>
         <br><br>
        en cas d'âge <02 ans Face antérolatéral externe de la cuisse droite et gauche<br>
        </p>
        <p style="text-align: right; margin-top: 30px;">
        Medecin traitant <br>
        <span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>&nbsp&nbsp&nbsp&nbsp&nbsp;
        </p>
    </div>

    <div class="print-button">
        <button id="printButton">Imprimer le schéma</button>
		<button id="saveButtonEssen2">Sauvegarder</button>
    </div>
   


    <script>
    function sauvegarderCertificatAntirabique() {
        // Récupérer les informations du patient depuis les champs du certificat affiché
        const inputs = document.querySelectorAll('input[type="text"]');
        const patientNomPrenom = inputs[0] ? inputs[0].value : ''; // Premier input = NOM
        const patientDateNaissance = inputs[1] ? inputs[1].value : ''; // Deuxième input = Date de naissance
        const patientAnimal = inputs[2] ? inputs[2].value : 'chien'; // Troisième input = Animal
        const docteurSpan = document.querySelector('.docteur');
        const docteur = docteurSpan ? docteurSpan.textContent.replace('Dr ', '').trim() : '';
        
        // Préparer les données pour l'API
        const maintenant = new Date();
        const data = {
            nom: patientNomPrenom.split(' ').slice(0, -1).join(' ').trim() || patientNomPrenom, // Extraire le nom
            prenom: patientNomPrenom.split(' ').slice(-1).join(' ').trim() || '', // Extraire le prénom
            medecin: docteur,
            classe: "classe02",
            type_de_vaccin: "Cellulaire",
            shema: "Essen",
            date_de_certificat: maintenant.toISOString().split('T')[0], // Date d'aujourd'hui
            date_de_naissance: patientDateNaissance,
            animal: patientAnimal,
            heure_creation: maintenant.toTimeString().split(' ')[0] // Ajouter l'heure pour éviter les doublons
        };
        
        // Envoyer les données à l'API
        fetch('http://localhost:5000/api/ajouter_antirabique', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(data)
        })
        .then(response => response.json())
        .then(result => {
            if (result.success) {
                alert('Certificat antirabique sauvegardé avec succès!');
            } else {
                alert('Erreur lors de la sauvegarde: ' + result.message);
            }
        })
        .catch(error => {
            console.error('Erreur:', error);
            alert('Erreur de connexion au serveur');
        });
    }

    const polycliniqueInput = document.getElementById('polyclinique');
    if (polycliniqueInput) {
        polycliniqueInput.addEventListener('input', function () {
            localStorage.setItem('polyclinique', this.value);
        });
    }

    const polycliniqueArInput = document.getElementById('polyclinique-ar');
    if (polycliniqueArInput) {
        polycliniqueArInput.addEventListener('input', function () {
            localStorage.setItem('polyclinique-ar', this.value);
        });
    }

    document.getElementById('printButton').addEventListener('click', function () {
        window.print();
    });
    
    document.getElementById('saveButtonEssen2').addEventListener('click', function () {
        sauvegarderCertificatAntirabique();
    });
    </script>   
    </body>
    </html>
    `;

    var newWindow = window.open("", "_blank");
    if (newWindow) {
        newWindow.document.write(certificatContent);
        newWindow.document.close();
    } else {
        console.log("Popup bloquée par le navigateur.");
    }
}

function risqueHemorragiqueClasse2(dateMorsure, poidsInput) {
    // Récupérer les informations du patient depuis le formulaire
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '';
    const patientAge = document.getElementById('patientAge').value || '';
    const patientDateNaissance = document.getElementById('patientDateNaissance').value || '';
    
    // Utiliser la date de naissance si disponible, sinon l'âge
    let ageInfo = '';
    if (patientDateNaissance) {
        ageInfo = patientDateNaissance;
    } else if (patientAge) {
        ageInfo = patientAge;
    } else {
        ageInfo = '[Date de naissance]';
    }
    
    // Calculer les dates pour le schéma Risque Hémorragique (J0, J7, J21, J28)
    const dateJour0 = new Date(dateMorsure);
    const datePlus7 = new Date(dateJour0);
    datePlus7.setDate(dateJour0.getDate() + 7);
    const datePlus21 = new Date(dateJour0);
    datePlus21.setDate(dateJour0.getDate() + 21);
    const datePlus3 = new Date(dateJour0);
    datePlus3.setDate(dateJour0.getDate() + 3);

    const dateFormattedJour0 = `${dateJour0.getFullYear()}-${String(dateJour0.getMonth() + 1).padStart(2, '0')}-${String(dateJour0.getDate()).padStart(2, '0')}`;
    const dateFormattedPlus7 = `${datePlus7.getFullYear()}-${String(datePlus7.getMonth() + 1).padStart(2, '0')}-${String(datePlus7.getDate()).padStart(2, '0')}`;
    const dateFormattedPlus21 = `${datePlus21.getFullYear()}-${String(datePlus21.getMonth() + 1).padStart(2, '0')}-${String(datePlus21.getDate()).padStart(2, '0')}`;
    const dateFormattedPlus3 = `${datePlus3.getFullYear()}-${String(datePlus3.getMonth() + 1).padStart(2, '0')}-${String(datePlus3.getDate()).padStart(2, '0')}`;

    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";
    const docteur = localStorage.getItem('docteur') || "";
    const animal = "chien"; // Animal en cause (valeur par défaut)

    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';

    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"></div>';
    }

    // Générer le certificat Risque Hémorragique
    const certificatContent = `
    <!DOCTYPE html>
    <html lang="fr">
    <head>
    <meta charset="UTF-8">
    <title>Certificat Antirabique - Risque Hémorragique/Qte Limitées</title>
    <style>
    body {
        font-family: Arial, sans-serif;
        padding: 20px;
        background-color: #f9f9f9;
    }
    .certificat {
        background-color: white;
        border: 1px solid #ddd;
        padding: 20px;
        max-width: 600px;
        margin: 0 auto;
        margin-top: 60px;
        box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
    }
    h1 {
        text-align: center;
        color: #333;
        text-decoration: underline;
        font-size: 16px;
    }
    h2 {
        text-align: center;
        color: #555;
        font-size: 10px;
        margin-top: 5px;
        margin-bottom: 15px;
    }
    p {
        line-height: 1.5;
        color: #555;
    }
    .small-text {
        font-size: 12px;
    }
    .print-button {
        text-align: center;
        margin-top: 20px;
    }
    .print-button button {
        padding: 10px 20px;
        font-size: 16px;
        background-color: #007bff;
        color: white;
        border: none;
        border-radius: 5px;
        cursor: pointer;
    }
    .print-button button:hover {
        background-color: #0056b3;
    }
    @media print {
        @page {
            size: A5;
            margin: 0.2cm 0.2cm 0.2cm 0.2cm;
        }
        body {
            margin: 0 !important;
            padding: 0 !important;
            font-size: 10pt !important;
            background-color: white;
        }
        .certificat {
            padding: 2px 8px !important;
            max-width: 100% !important;
            border: none;
            box-shadow: none;
            margin-top: 0;
        }
        h1 {
            font-size: 14pt !important;
            margin: 5px 0 !important;
            margin-top: 2cm !important;
        }
        h2 {
            font-size: 12pt !important;
            margin: 3px 0 !important;
        }
        p {
            font-size: 9pt !important;
            margin: 2px 0 !important;
            line-height: 1.2 !important;
        }
        input[type="text"],
        input[type="date"],
        textarea {
            border: none !important;
            background: none !important;
            box-shadow: none !important;
            outline: none !important;
            font-size: 9pt !important;
        }
        input[type="text"]:focus,
        input[type="date"]:focus,
        textarea:focus {
            border: none !important;
            outline: none !important;
        }
        .print-button {
            display: none;
        }
        .docteur {
            font-weight: bold;
            font-size: 14pt !important;
            margin-right: 50px;
        }
        /* Additional space optimization */
        * {
            margin-top: 0 !important;
            margin-bottom: 2px !important;
        }
    }
    </style>
    </head>
    <body>
   ${enteteContent}

    <div class="certificat">
        <h1>Schéma Antirabique - Classe 02 (Risque Hémorragique/Qte Limitées)</h1>
        <h2>Selon l'instruction N16 du 15 Juillet 2024 relative à la conduite à tenir devant un risque rabique</h2>
        <p>
        NOM : <strong><input type="text" value="${patientNomPrenom}" style="width: auto;"></strong><br>
        Date de naissance : <strong><input type="text" value="${ageInfo}" style="width: auto;"></strong><br>
        Animal en cause : <strong><input type="text" value="${animal}" style="width: auto;"></strong><br>
		<br>
     <span class="small-text">Schéma adapté pour patient à  risque hémorragique ou quantités limitées</span>
     <br>
        </p>
        <p>
         <br>
		 <strong>Personne sans antécédent de vaccination:</strong><br><br>
         Un total de six doses : 2 doses en deux [2] sites différents<br><br>
         Jour 0 : <input type="date" id="dateJour0" value="${dateFormattedJour0}" readonly> ( 2 doses en ID  ))<br>
         Jour 3 : <input type="date" id="datePlus3" value="${dateFormattedPlus3}" readonly> ( 2 doses en ID  )<br>
         Jour 7 : <input type="date" id="datePlus7" value="${dateFormattedPlus7}" readonly> ( 2 doses en ID  )<br>
       
         <br><br>
        NB: 01 dose=0,1mI (administration en ID)
        </p>
        <p style="text-align: right; margin-top: 30px;">
        Medecin traitant <br>
        <span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>&nbsp&nbsp&nbsp&nbsp&nbsp;
        </p>
    </div>

    <div class="print-button">
        <button id="printButton">Imprimer le schéma</button>
		<button id="saveButtonH2">Sauvegarder</button>
    </div>
    

    <script>
    function sauvegarderCertificatAntirabique() {
        // Récupérer les informations du patient depuis les champs du certificat affiché
        const inputs = document.querySelectorAll('input[type="text"]');
        const patientNomPrenom = inputs[0] ? inputs[0].value : ''; // Premier input = NOM
        const patientDateNaissance = inputs[1] ? inputs[1].value : ''; // Deuxième input = Date de naissance
        const patientAnimal = inputs[2] ? inputs[2].value : 'chien'; // Troisième input = Animal
        const docteurSpan = document.querySelector('.docteur');
        const docteur = docteurSpan ? docteurSpan.textContent.replace('Dr ', '').trim() : '';
        
        // Préparer les données pour l'API
        const maintenant = new Date();
        const data = {
            nom: patientNomPrenom.split(' ').slice(0, -1).join(' ').trim() || patientNomPrenom, // Extraire le nom
            prenom: patientNomPrenom.split(' ').slice(-1).join(' ').trim() || '', // Extraire le prénom
            medecin: docteur,
            classe: "classe02",
            type_de_vaccin: "Cellulaire",
            shema: "Risque Hémorragique",
            date_de_certificat: maintenant.toISOString().split('T')[0], // Date d'aujourd'hui
            date_de_naissance: patientDateNaissance,
            animal: patientAnimal,
            heure_creation: maintenant.toTimeString().split(' ')[0] // Ajouter l'heure pour éviter les doublons
        };
        
        // Envoyer les données à l'API
        fetch('http://localhost:5000/api/ajouter_antirabique', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(data)
        })
        .then(response => response.json())
        .then(result => {
            if (result.success) {
                alert('Certificat antirabique sauvegardé avec succès!');
            } else {
                alert('Erreur lors de la sauvegarde: ' + result.message);
            }
        })
        .catch(error => {
            console.error('Erreur:', error);
            alert('Erreur de connexion au serveur');
        });
    }

    const polycliniqueInput = document.getElementById('polyclinique');
    if (polycliniqueInput) {
        polycliniqueInput.addEventListener('input', function () {
            localStorage.setItem('polyclinique', this.value);
        });
    }

    const polycliniqueArInput = document.getElementById('polyclinique-ar');
    if (polycliniqueArInput) {
        polycliniqueArInput.addEventListener('input', function () {
            localStorage.setItem('polyclinique-ar', this.value);
        });
    }

    document.getElementById('printButton').addEventListener('click', function () {
        window.print();
    });
    
    document.getElementById('saveButtonH2').addEventListener('click', function () {
        sauvegarderCertificatAntirabique();
    });
    </script> 
    </body>
    </html>
    `;

    var newWindow = window.open("", "_blank");
    if (newWindow) {
        newWindow.document.write(certificatContent);
        newWindow.document.close();
    } else {
        console.log("Popup bloquée par le navigateur.");
    }
}

function prophylaxiePreExpositionSchema1Classe2(dateMorsure, poidsInput) {
    // Récupérer les informations du patient depuis le formulaire
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '';
    const patientAge = document.getElementById('patientAge').value || '';
    const patientDateNaissance = document.getElementById('patientDateNaissance').value || '';
    
    // Utiliser la date de naissance si disponible, sinon l'âge
    let ageInfo = '';
    if (patientDateNaissance) {
        ageInfo = patientDateNaissance;
    } else if (patientAge) {
        ageInfo = patientAge;
    } else {
        ageInfo = '[Date de naissance]';
    }
    
    // Calculer les dates pour le schéma ATCD Vaccinaux Schéma 1 (J0, J3)
    const dateJour0 = new Date(dateMorsure);
    const datePlus3 = new Date(dateJour0);
    datePlus3.setDate(dateJour0.getDate() + 3);

    const dateFormattedJour0 = `${dateJour0.getFullYear()}-${String(dateJour0.getMonth() + 1).padStart(2, '0')}-${String(dateJour0.getDate()).padStart(2, '0')}`;
    const dateFormattedPlus3 = `${datePlus3.getFullYear()}-${String(datePlus3.getMonth() + 1).padStart(2, '0')}-${String(datePlus3.getDate()).padStart(2, '0')}`;

    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";
    const docteur = localStorage.getItem('docteur') || "";
    const animal = "chien"; // Animal en cause (valeur par défaut)

    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';

    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"></div>';
    }

    // Générer le certificat ATCD Vaccinaux Schéma 1
    const certificatContent = `
    <!DOCTYPE html>
    <html lang="fr">
    <head>
    <meta charset="UTF-8">
    <title>Certificat Antirabique - Avec ATCD Vaccinaux (Schéma 1)</title>
    <style>
    body {
        font-family: Arial, sans-serif;
        padding: 20px;
        background-color: #f9f9f9;
    }
    .certificat {
        background-color: white;
        border: 1px solid #ddd;
        padding: 20px;
        max-width: 600px;
        margin: 0 auto;
        margin-top: 60px;
        box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
    }
    h1 {
        text-align: center;
        color: #333;
        text-decoration: underline;
        font-size: 16px;
    }
    h2 {
        text-align: center;
        color: #555;
        font-size: 10px;
        margin-top: 5px;
        margin-bottom: 15px;
    }
    p {
        line-height: 1.5;
        color: #555;
    }
    .small-text {
        font-size: 12px;
    }
    .print-button {
        text-align: center;
        margin-top: 20px;
    }
    .print-button button {
        padding: 10px 20px;
        font-size: 16px;
        background-color: #007bff;
        color: white;
        border: none;
        border-radius: 5px;
        cursor: pointer;
    }
    .print-button button:hover {
        background-color: #0056b3;
    }
    @media print {
        @page {
            size: A5;
            margin: 0.2cm 0.2cm 0.2cm 0.2cm;
        }
        body {
            margin: 0 !important;
            padding: 0 !important;
            font-size: 10pt !important;
            background-color: white;
        }
        .certificat {
            padding: 2px 8px !important;
            max-width: 100% !important;
            border: none;
            box-shadow: none;
            margin-top: 0;
        }
        h1 {
            font-size: 14pt !important;
            margin: 5px 0 !important;
            margin-top: 2cm !important;
        }
        h2 {
            font-size: 12pt !important;
            margin: 3px 0 !important;
        }
        p {
            font-size: 9pt !important;
            margin: 2px 0 !important;
            line-height: 1.2 !important;
        }
        input[type="text"],
        input[type="date"],
        textarea {
            border: none !important;
            background: none !important;
            box-shadow: none !important;
            outline: none !important;
            font-size: 9pt !important;
        }
        input[type="text"]:focus,
        input[type="date"]:focus,
        textarea:focus {
            border: none !important;
            outline: none !important;
        }
        .print-button {
            display: none;
        }
        .docteur {
            font-weight: bold;
            font-size: 14pt !important;
            margin-right: 50px;
        }
        /* Additional space optimization */
        * {
            margin-top: 0 !important;
            margin-bottom: 2px !important;
        }
    }
    </style>
    </head>
    <body>
   ${enteteContent}

    <div class="certificat">
        <h1>Schéma Antirabique - Avec ATCD Vaccinaux (Schéma 1)</h1>
        <h2>Selon l'instruction N16 du 15 Juillet 2024 relative à la conduite à tenir devant un risque rabique</h2>
        <p>
        NOM : <strong><input type="text" value="${patientNomPrenom}" style="width: auto;"></strong><br>
        Date de naissance : <strong><input type="text" value="${ageInfo}" style="width: auto;"></strong><br>
        Animal en cause : <strong><input type="text" value="${animal}" style="width: auto;"></strong><br>
        </p>
        <p>
         <strong style="font-size: 14px;">Personne ayant reçu une prophylaxie Pré-exPosition, ou (>= 02 doses):</strong><br>
         <br><br>
         Schéma 1 : Un total de deux (2) doses<br><br>
         Jour 0 : <input type="date" id="dateJour0" value="${dateFormattedJour0}" readonly> (une (1) dose ID en un seul site)<br>
         Jour 3 : <input type="date" id="datePlus3" value="${dateFormattedPlus3}" readonly> (une (1) dose ID en un seul site)<br>
         <br><br>
        NB: 01 dose=0,1mI
        </p>
        <p style="text-align: right; margin-top: 30px;">
        Medecin traitant <br>
        <span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>&nbsp&nbsp&nbsp&nbsp&nbsp;
        </p>
    </div>

    <div class="print-button">
        <button id="printButton">Imprimer le schéma</button>
		<button id="saveButtonAtcdv">Sauvegarder</button>
    </div>
    

    <script>
    function sauvegarderCertificatAntirabique() {
        // Récupérer les informations du patient depuis les champs du certificat affiché
        const inputs = document.querySelectorAll('input[type="text"]');
        const patientNomPrenom = inputs[0] ? inputs[0].value : ''; // Premier input = NOM
        const patientDateNaissance = inputs[1] ? inputs[1].value : ''; // Deuxième input = Date de naissance
        const patientAnimal = inputs[2] ? inputs[2].value : 'chien'; // Troisième input = Animal
        const docteurSpan = document.querySelector('.docteur');
        const docteur = docteurSpan ? docteurSpan.textContent.replace('Dr ', '').trim() : '';
        
        // Préparer les données pour l'API
        const maintenant = new Date();
        const data = {
            nom: patientNomPrenom.split(' ').slice(0, -1).join(' ').trim() || patientNomPrenom, // Extraire le nom
            prenom: patientNomPrenom.split(' ').slice(-1).join(' ').trim() || '', // Extraire le prénom
            medecin: docteur,
            classe: "classe02",
            type_de_vaccin: "Cellulaire",
            shema: " Avec ATCD 1",
            date_de_certificat: maintenant.toISOString().split('T')[0], // Date d'aujourd'hui
            date_de_naissance: patientDateNaissance,
            animal: patientAnimal,
            heure_creation: maintenant.toTimeString().split(' ')[0] // Ajouter l'heure pour éviter les doublons
        };
        
        // Envoyer les données à l'API
        fetch('http://localhost:5000/api/ajouter_antirabique', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(data)
        })
        .then(response => response.json())
        .then(result => {
            if (result.success) {
                alert('Certificat antirabique sauvegardé avec succès!');
            } else {
                alert('Erreur lors de la sauvegarde: ' + result.message);
            }
        })
        .catch(error => {
            console.error('Erreur:', error);
            alert('Erreur de connexion au serveur');
        });
    }

    const polycliniqueInput = document.getElementById('polyclinique');
    if (polycliniqueInput) {
        polycliniqueInput.addEventListener('input', function () {
            localStorage.setItem('polyclinique', this.value);
        });
    }

    const polycliniqueArInput = document.getElementById('polyclinique-ar');
    if (polycliniqueArInput) {
        polycliniqueArInput.addEventListener('input', function () {
            localStorage.setItem('polyclinique-ar', this.value);
        });
    }

    document.getElementById('printButton').addEventListener('click', function () {
        window.print();
    });
    
    document.getElementById('saveButtonAtcdv').addEventListener('click', function () {
        sauvegarderCertificatAntirabique();
    });
    </script> 
    </body>
    </html>
    `;

    var newWindow = window.open("", "_blank");
    if (newWindow) {
        newWindow.document.write(certificatContent);
        newWindow.document.close();
    } else {
        console.log("Popup bloquée par le navigateur.");
    }
}

function prophylaxiePreExpositionSchema2Classe2(dateMorsure) {
    // Récupérer les informations du patient depuis le formulaire
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '';
    const patientAge = document.getElementById('patientAge').value || '';
    const patientDateNaissance = document.getElementById('patientDateNaissance').value || '';
    
    // Utiliser la date de naissance si disponible, sinon l'âge
    let ageInfo = '';
    if (patientDateNaissance) {
        ageInfo = patientDateNaissance;
    } else if (patientAge) {
        ageInfo = patientAge;
    } else {
        ageInfo = '[Date de naissance]';
    }
    
    // Calculer les dates pour le schéma ATCD Vaccinaux Schéma 2 (J0 uniquement)
    const dateJour0 = new Date(dateMorsure);

    const dateFormattedJour0 = `${dateJour0.getFullYear()}-${String(dateJour0.getMonth() + 1).padStart(2, '0')}-${String(dateJour0.getDate()).padStart(2, '0')}`;

    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";
    const docteur = localStorage.getItem('docteur') || "";
    const animal = "chien"; // Animal en cause (valeur par défaut)

    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';

    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"></div>';
    }

    // Générer le certificat ATCD Vaccinaux Schéma 2
    const certificatContent = `
    <!DOCTYPE html>
    <html lang="fr">
    <head>
    <meta charset="UTF-8">
    <title>Certificat Antirabique - Avec ATCD Vaccinaux (Schéma 2)</title>
    <style>
    body {
        font-family: Arial, sans-serif;
        padding: 20px;
        background-color: #f9f9f9;
    }
    .certificat {
        background-color: white;
        border: 1px solid #ddd;
        padding: 20px;
        max-width: 600px;
        margin: 0 auto;
        margin-top: 60px;
        box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
    }
    h1 {
        text-align: center;
        color: #333;
        text-decoration: underline;
        font-size: 16px;
    }
    h2 {
        text-align: center;
        color: #555;
        font-size: 10px;
        margin-top: 5px;
        margin-bottom: 15px;
    }
    p {
        line-height: 1.5;
        color: #555;
    }
    .small-text {
        font-size: 12px;
    }
    .print-button {
        text-align: center;
        margin-top: 20px;
    }
    .print-button button {
        padding: 10px 20px;
        font-size: 16px;
        background-color: #007bff;
        color: white;
        border: none;
        border-radius: 5px;
        cursor: pointer;
    }
    .print-button button:hover {
        background-color: #0056b3;
    }
    @media print {
        @page {
            size: A5;
            margin: 0.2cm 0.2cm 0.2cm 0.2cm;
        }
        body {
            margin: 0 !important;
            padding: 0 !important;
            font-size: 10pt !important;
            background-color: white;
        }
        .certificat {
            padding: 2px 8px !important;
            max-width: 100% !important;
            border: none;
            box-shadow: none;
            margin-top: 0;
        }
        h1 {
            font-size: 14pt !important;
            margin: 5px 0 !important;
            margin-top: 2cm !important;
        }
        h2 {
            font-size: 12pt !important;
            margin: 3px 0 !important;
        }
        p {
            font-size: 9pt !important;
            margin: 2px 0 !important;
            line-height: 1.2 !important;
        }
        input[type="text"],
        input[type="date"],
        textarea {
            border: none !important;
            background: none !important;
            box-shadow: none !important;
            outline: none !important;
            font-size: 9pt !important;
        }
        input[type="text"]:focus,
        input[type="date"]:focus,
        textarea:focus {
            border: none !important;
            outline: none !important;
        }
        .print-button {
            display: none;
        }
        .docteur {
            font-weight: bold;
            font-size: 14pt !important;
            margin-right: 50px;
        }
        /* Additional space optimization */
        * {
            margin-top: 0 !important;
            margin-bottom: 2px !important;
        }
    }
    </style>
    </head>
    <body>
   ${enteteContent}

    <div class="certificat">
        <h1>Schéma Antirabique - Avec ATCD Vaccinaux (Schéma 2)</h1>
        <h2>Selon l'instruction N16 du 15 Juillet 2024 relative à la conduite à tenir devant un risque rabique</h2>
        <p>
        NOM : <strong><input type="text" value="${patientNomPrenom}" style="width: auto;"></strong><br>
        Date de naissance : <strong><input type="text" value="${ageInfo}" style="width: auto;"></strong><br>
        Animal en cause : <strong><input type="text" value="${animal}" style="width: auto;"></strong><br>
        </p>
        <p>
         <strong>Personne ayant reçu une prophylaxie Pré-exPosition, ou (≥ 02 doses):</strong><br>
         Schéma 2 : Un total de quatre (4) doses<br><br>
         Jour 0 : <input type="date" id="dateJour0" value="${dateFormattedJour0}" readonly> (une dose en ID en quatre (4) sites différents)<br>
         <br><br>
        NB: 01 dose=0,1mI
        </p>
        <p style="text-align: right; margin-top: 30px;">
        Medecin traitant <br>
        <span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>&nbsp&nbsp&nbsp&nbsp&nbsp;
        </p>
    </div>

    <div class="print-button">
        <button id="printButton">Imprimer le schéma</button>
		<button id="saveButtonAtcdv2">Sauvegarder</button>
    </div>
  

    <script>
    function sauvegarderCertificatAntirabique() {
        // Récupérer les informations du patient depuis les champs du certificat affiché
        const inputs = document.querySelectorAll('input[type="text"]');
        const patientNomPrenom = inputs[0] ? inputs[0].value : ''; // Premier input = NOM
        const patientDateNaissance = inputs[1] ? inputs[1].value : ''; // Deuxième input = Date de naissance
        const patientAnimal = inputs[2] ? inputs[2].value : 'chien'; // Troisième input = Animal
        const docteurSpan = document.querySelector('.docteur');
        const docteur = docteurSpan ? docteurSpan.textContent.replace('Dr ', '').trim() : '';
        
        // Préparer les données pour l'API
        const maintenant = new Date();
        const data = {
            nom: patientNomPrenom.split(' ').slice(0, -1).join(' ').trim() || patientNomPrenom, // Extraire le nom
            prenom: patientNomPrenom.split(' ').slice(-1).join(' ').trim() || '', // Extraire le prénom
            medecin: docteur,
            classe: "classe02",
            type_de_vaccin: "Cellulaire",
            shema: " Avec ATCD 2",
            date_de_certificat: maintenant.toISOString().split('T')[0], // Date d'aujourd'hui
            date_de_naissance: patientDateNaissance,
            animal: patientAnimal,
            heure_creation: maintenant.toTimeString().split(' ')[0] // Ajouter l'heure pour éviter les doublons
        };
        
        // Envoyer les données à l'API
        fetch('http://localhost:5000/api/ajouter_antirabique', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(data)
        })
        .then(response => response.json())
        .then(result => {
            if (result.success) {
                alert('Certificat antirabique sauvegardé avec succès!');
            } else {
                alert('Erreur lors de la sauvegarde: ' + result.message);
            }
        })
        .catch(error => {
            console.error('Erreur:', error);
            alert('Erreur de connexion au serveur');
        });
    }

    const polycliniqueInput = document.getElementById('polyclinique');
    if (polycliniqueInput) {
        polycliniqueInput.addEventListener('input', function () {
            localStorage.setItem('polyclinique', this.value);
        });
    }

    const polycliniqueArInput = document.getElementById('polyclinique-ar');
    if (polycliniqueArInput) {
        polycliniqueArInput.addEventListener('input', function () {
            localStorage.setItem('polyclinique-ar', this.value);
        });
    }

    document.getElementById('printButton').addEventListener('click', function () {
        window.print();
    });
    
    document.getElementById('saveButtonAtcdv2').addEventListener('click', function () {
        sauvegarderCertificatAntirabique();
    });
    </script> 
    </body>
    </html>
    `;

    var newWindow = window.open("", "_blank");
    if (newWindow) {
        newWindow.document.write(certificatContent);
        newWindow.document.close();
    } else {
        console.log("Popup bloquée par le navigateur.");
    }
}

function vaccinc3(dateMorsure, poidsInput) {
    // Récupérer les informations du patient depuis le formulaire
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '';
    const patientAge = document.getElementById('patientAge').value || '';
    const patientDateNaissance = document.getElementById('patientDateNaissance').value || '';
    
    // Utiliser la date de naissance si disponible, sinon l'âge
    let ageInfo = '';
    if (patientDateNaissance) {
        ageInfo = patientDateNaissance;
    } else if (patientAge) {
        ageInfo = patientAge;
    } else {
        ageInfo = '[Date de naissance]';
    }
    
    // Calculer le SAR (Sérum Antirabique) en fonction du poids
    const poids = parseFloat(poidsInput) || 0;
    let sar = poids / 5;
    if (poids >= 75) {
        sar = 15;
    }
    sar = Math.round(sar * 100) / 100;
    
    // Calculer les dates pour le schéma Zagreb 3 (J0, J7, J21)
    const dateJour0 = new Date(dateMorsure);
    const datePlus7 = new Date(dateJour0);
    datePlus7.setDate(dateJour0.getDate() + 7);
    const datePlus21 = new Date(dateJour0);
    datePlus21.setDate(dateJour0.getDate() + 21);

    const dateFormattedJour0 = `${dateJour0.getFullYear()}-${String(dateJour0.getMonth() + 1).padStart(2, '0')}-${String(dateJour0.getDate()).padStart(2, '0')}`;
    const dateFormattedPlus7 = `${datePlus7.getFullYear()}-${String(datePlus7.getMonth() + 1).padStart(2, '0')}-${String(datePlus7.getDate()).padStart(2, '0')}`;
    const dateFormattedPlus21 = `${datePlus21.getFullYear()}-${String(datePlus21.getMonth() + 1).padStart(2, '0')}-${String(datePlus21.getDate()).padStart(2, '0')}`;

    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";
    const docteur = localStorage.getItem('docteur') || "";
    const animal = "chien"; // Animal en cause (valeur par défaut)

    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';

    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"></div>';
    }

    // Générer le certificat Vaccin C Schema 3
    const certificatContent = `
    <!DOCTYPE html>
    <html lang="fr">
    <head>
    <meta charset="UTF-8">
    <title>Certificat Antirabique - Vaccin C Schema 3 (Zagreb)</title>
    <style>
    body {
        font-family: Arial, sans-serif;
        padding: 20px;
        background-color: #f9f9f9;
    }
    .certificat {
        background-color: white;
        border: 1px solid #ddd;
        padding: 20px;
        max-width: 600px;
        margin: 0 auto;
        margin-top: 60px;
        box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
    }
    h1 {
        text-align: center;
        color: #333;
        text-decoration: underline;
        font-size: 16px;
    }
    h2 {
        text-align: center;
        color: #555;
        font-size: 10px;
        margin-top: 5px;
        margin-bottom: 15px;
    }
    p {
        line-height: 1.5;
        color: #555;
    }
    .small-text {
        font-size: 12px;
    }
    .print-button {
        text-align: center;
        margin-top: 20px;
    }
    .print-button button {
        padding: 10px 20px;
        font-size: 16px;
        background-color: #007bff;
        color: white;
        border: none;
        border-radius: 5px;
        cursor: pointer;
    }
    .print-button button:hover {
        background-color: #0056b3;
    }
    @media print {
        @page {
            size: A5;
            margin: 0.2cm 0.2cm 0.2cm 0.2cm;
        }
        body {
            margin: 0 !important;
            padding: 0 !important;
            font-size: 10pt !important;
            background-color: white;
        }
        .certificat {
            padding: 2px 8px !important;
            max-width: 100% !important;
            border: none;
            box-shadow: none;
            margin-top: 0;
        }
        h1 {
            font-size: 14pt !important;
            margin: 5px 0 !important;
            margin-top: 2cm !important;
        }
        h2 {
            font-size: 12pt !important;
            margin: 3px 0 !important;
        }
        p {
            font-size: 9pt !important;
            margin: 2px 0 !important;
            line-height: 1.2 !important;
        }
        input[type="text"],
        input[type="date"],
        textarea {
            border: none !important;
            background: none !important;
            box-shadow: none !important;
            outline: none !important;
            font-size: 9pt !important;
        }
        input[type="text"]:focus,
        input[type="date"]:focus,
        textarea:focus {
            border: none !important;
            outline: none !important;
        }
        .print-button {
            display: none;
        }
        .docteur {
            font-weight: bold;
            font-size: 14pt !important;
            margin-right: 50px;
        }
        /* Additional space optimization */
        * {
            margin-top: 0 !important;
            margin-bottom: 2px !important;
        }
    }
    </style>
    </head>
    <body>
   ${enteteContent}

    <div class="certificat">
        <h1>Schéma Antirabique - Classe 03 (Vaccin C Schema 3)</h1>
        <h2>Selon l'instruction N16 du 15 Juillet 2024 relative à la conduite à tenir devant un risque rabique</h2>
        <p>
        NOM : <strong><input type="text" value="${patientNomPrenom}" style="width: auto;"></strong><br>
        Date de naissance : <strong><input type="text" value="${ageInfo}" style="width: auto;"></strong><br>
        Animal en cause : <strong><input type="text" value="${animal}" style="width: auto;"></strong><br>
        Poids : <strong><input type="text" value="${poidsInput} kg" style="width: auto;"></strong><br>
        SAR : <strong><input type="text" value="${sar} cc" style="width: auto;"></strong><br><br>		
        Classe 03, schéma choisi : vaccin cellulaire / schéma de Zagreb / avec SAR
        <br>
        </p>
        <p>
         <br><br>
         Jour 0 : <input type="date" id="dateJour0" value="${dateFormattedJour0}" readonly>( 02 doses chacune dans un deltoïde )<br>
         Jour 7 : <input type="date" id="datePlus7" value="${dateFormattedPlus7}" readonly> <br>
         Jour 21 : <input type="date" id="datePlus21" value="${dateFormattedPlus21}" readonly> <br>
         <br><br>
         en cas d'âge <02 ans Face antérolatéral externe de la cuisse droite et gauche<br>
        </p>
        <p style="text-align: right; margin-top: 30px;">
        Medecin traitant <br>
        <span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>&nbsp&nbsp&nbsp&nbsp&nbsp;
        </p>
    </div>

    <div class="print-button">
        <button id="printButton">Imprimer le schéma</button>
		<button id="saveButtonc3">Sauvegarder</button>
    </div>

    <script>
    function sauvegarderCertificatAntirabique() {
        // Récupérer les informations du patient depuis les champs du certificat affiché
        const inputs = document.querySelectorAll('input[type="text"]');
        const patientNomPrenom = inputs[0] ? inputs[0].value : ''; // Premier input = NOM
        const patientDateNaissance = inputs[1] ? inputs[1].value : ''; // Deuxième input = Date de naissance
        const patientAnimal = inputs[2] ? inputs[2].value : 'chien'; // Troisième input = Animal
        const docteurSpan = document.querySelector('.docteur');
        const docteur = docteurSpan ? docteurSpan.textContent.replace('Dr ', '').trim() : '';
        
        // Préparer les données pour l'API
        const maintenant = new Date();
        const data = {
            nom: patientNomPrenom.split(' ').slice(0, -1).join(' ').trim() || patientNomPrenom, // Extraire le nom
            prenom: patientNomPrenom.split(' ').slice(-1).join(' ').trim() || '', // Extraire le prénom
            medecin: docteur,
            classe: "classe03",
            type_de_vaccin: "Cellulaire",
            shema: "Zagreb",
            date_de_certificat: maintenant.toISOString().split('T')[0], // Date d'aujourd'hui
            date_de_naissance: patientDateNaissance,
            animal: patientAnimal,
            heure_creation: maintenant.toTimeString().split(' ')[0] // Ajouter l'heure pour éviter les doublons
        };
        
        // Envoyer les données à l'API
        fetch('http://localhost:5000/api/ajouter_antirabique', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(data)
        })
        .then(response => response.json())
        .then(result => {
            if (result.success) {
                alert('Certificat antirabique sauvegardé avec succès!');
            } else {
                alert('Erreur lors de la sauvegarde: ' + result.message);
            }
        })
        .catch(error => {
            console.error('Erreur:', error);
            alert('Erreur de connexion au serveur');
        });
    }

    const polycliniqueInput = document.getElementById('polyclinique');
    if (polycliniqueInput) {
        polycliniqueInput.addEventListener('input', function () {
            localStorage.setItem('polyclinique', this.value);
        });
    }

    const polycliniqueArInput = document.getElementById('polyclinique-ar');
    if (polycliniqueArInput) {
        polycliniqueArInput.addEventListener('input', function () {
            localStorage.setItem('polyclinique-ar', this.value);
        });
    }

    document.getElementById('printButton').addEventListener('click', function () {
        window.print();
    });
    
    document.getElementById('saveButtonc3').addEventListener('click', function () {
        sauvegarderCertificatAntirabique();
    });
    </script> 
    </body>
    </html>
    `;

    var newWindow = window.open("", "_blank");
    if (newWindow) {
        newWindow.document.write(certificatContent);
        newWindow.document.close();
    } else {
        console.log("Popup bloquée par le navigateur.");
    }
}

function essen3(dateMorsure, poidsInput) {
    // Récupérer les informations du patient depuis le formulaire
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '';
    const patientAge = document.getElementById('patientAge').value || '';
    const patientDateNaissance = document.getElementById('patientDateNaissance').value || '';
    
    // Utiliser la date de naissance si disponible, sinon l'âge
    let ageInfo = '';
    if (patientDateNaissance) {
        ageInfo = patientDateNaissance;
    } else if (patientAge) {
        ageInfo = patientAge;
    } else {
        ageInfo = '[Date de naissance]';
    }
    
    // Calculer le SAR (Sérum Antirabique) en fonction du poids
    const poids = parseFloat(poidsInput) || 0;
    let sar = poids / 5;
    if (poids >= 75) {
        sar = 15;
    }
    sar = Math.round(sar * 100) / 100;
    
    // Calculer les dates pour le schéma Essen 3 (J0, J3, J7, J14)
    const dateJour0 = new Date(dateMorsure);
    const datePlus3 = new Date(dateJour0);
    datePlus3.setDate(dateJour0.getDate() + 3);
    const datePlus7 = new Date(dateJour0);
    datePlus7.setDate(dateJour0.getDate() + 7);
    const datePlus14 = new Date(dateJour0);
    datePlus14.setDate(dateJour0.getDate() + 14);

    const dateFormattedJour0 = `${dateJour0.getFullYear()}-${String(dateJour0.getMonth() + 1).padStart(2, '0')}-${String(dateJour0.getDate()).padStart(2, '0')}`;
    const dateFormattedPlus3 = `${datePlus3.getFullYear()}-${String(datePlus3.getMonth() + 1).padStart(2, '0')}-${String(datePlus3.getDate()).padStart(2, '0')}`;
    const dateFormattedPlus7 = `${datePlus7.getFullYear()}-${String(datePlus7.getMonth() + 1).padStart(2, '0')}-${String(datePlus7.getDate()).padStart(2, '0')}`;
    const dateFormattedPlus14 = `${datePlus14.getFullYear()}-${String(datePlus14.getMonth() + 1).padStart(2, '0')}-${String(datePlus14.getDate()).padStart(2, '0')}`;

    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";
    const docteur = localStorage.getItem('docteur') || "";
    const animal = "chien"; // Animal en cause (valeur par défaut)

    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';

    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"></div>';
    }

    // Générer le certificat Essen Schema 3
    const certificatContent = `
    <!DOCTYPE html>
    <html lang="fr">
    <head>
    <meta charset="UTF-8">
    <title>Certificat Antirabique - Essen Schema 3</title>
    <style>
    body {
        font-family: Arial, sans-serif;
        padding: 20px;
        background-color: #f9f9f9;
    }
    .certificat {
        background-color: white;
        border: 1px solid #ddd;
        padding: 20px;
        max-width: 600px;
        margin: 0 auto;
        margin-top: 60px;
        box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
    }
    h1 {
        text-align: center;
        color: #333;
        text-decoration: underline;
        font-size: 16px;
    }
    h2 {
        text-align: center;
        color: #555;
        font-size: 10px;
        margin-top: 5px;
        margin-bottom: 15px;
    }
    p {
        line-height: 1.5;
        color: #555;
    }
    .small-text {
        font-size: 12px;
    }
    .print-button {
        text-align: center;
        margin-top: 20px;
    }
    .print-button button {
        padding: 10px 20px;
        font-size: 16px;
        background-color: #007bff;
        color: white;
        border: none;
        border-radius: 5px;
        cursor: pointer;
    }
    .print-button button:hover {
        background-color: #0056b3;
    }
    @media print {
        @page {
            size: A5;
            margin: 0.2cm 0.2cm 0.2cm 0.2cm;
        }
        body {
            margin: 0 !important;
            padding: 0 !important;
            font-size: 10pt !important;
            background-color: white;
        }
        .certificat {
            padding: 2px 8px !important;
            max-width: 100% !important;
            border: none;
            box-shadow: none;
            margin-top: 0;
        }
        h1 {
            font-size: 14pt !important;
            margin: 5px 0 !important;
            margin-top: 2cm !important;
        }
        h2 {
            font-size: 12pt !important;
            margin: 3px 0 !important;
        }
        p {
            font-size: 9pt !important;
            margin: 2px 0 !important;
            line-height: 1.2 !important;
        }
        input[type="text"],
        input[type="date"],
        textarea {
            border: none !important;
            background: none !important;
            box-shadow: none !important;
            outline: none !important;
            font-size: 9pt !important;
        }
        input[type="text"]:focus,
        input[type="date"]:focus,
        textarea:focus {
            border: none !important;
            outline: none !important;
        }
        .print-button {
            display: none;
        }
        .docteur {
            font-weight: bold;
            font-size: 14pt !important;
            margin-right: 50px;
        }
        /* Additional space optimization */
        * {
            margin-top: 0 !important;
            margin-bottom: 2px !important;
        }
    }
    </style>
    </head>
    <body>
   ${enteteContent}

    <div class="certificat">
        <h1>Schéma Antirabique - Classe 03 (Essen)</h1>
        <h2>Selon l'instruction N16 du 15 Juillet 2024 relative à la conduite à tenir devant un risque rabique</h2>
        <p>
        NOM : <strong><input type="text" value="${patientNomPrenom}" style="width: auto;"></strong><br>
        Date de naissance : <strong><input type="text" value="${ageInfo}" style="width: auto;"></strong><br>
        Animal en cause : <strong><input type="text" value="${animal}" style="width: auto;"></strong><br>
        Poids : <strong><input type="text" value="${poidsInput} kg" style="width: auto;"></strong><br>
        SAR : <strong><input type="text" value="${sar} cc" style="width: auto;"></strong><br>
		<br>
         Classe 03, schéma choisi : vaccin cellulaire / schéma de Essen / avec SAR
        <br>
        </p>
        <p>
         <br>
         Jour 0 : <input type="date" id="dateJour0" value="${dateFormattedJour0}" readonly> ( dans le deltoïde )<br>
         Jour 3 : <input type="date" id="datePlus3" value="${dateFormattedPlus3}" readonly> <br>
         Jour 7 : <input type="date" id="datePlus7" value="${dateFormattedPlus7}" readonly> <br>
         Jour 14 : <input type="date" id="datePlus14" value="${dateFormattedPlus14}" readonly> <br>
         <br><br>
        Pour les immunodéprimées si sérologie de contrôle après 02 a 04 semaines de vaccin < 0.5 ui/ml (ou s’il n'est pas faisable) ajouter une autre dose de vaccin.<br>
		
		<br><br>
		<br><br>
        <span class="small-text">en cas d'âge <02 ans Face antérolatéral externe de la cuisse droite et gauche</span><br>
        </p>
        <p style="text-align: right; margin-top: 30px;">
        Medecin traitant <br>
        <span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>&nbsp&nbsp&nbsp&nbsp&nbsp;
        </p>
    </div>

    <div class="print-button">
        <button id="printButton">Imprimer le schéma</button>
		<button id="saveButtonessen3">Sauvegarder</button>
    </div>

    <script>
    function sauvegarderCertificatAntirabique() {
        // Récupérer les informations du patient depuis les champs du certificat affiché
        const inputs = document.querySelectorAll('input[type="text"]');
        const patientNomPrenom = inputs[0] ? inputs[0].value : ''; // Premier input = NOM
        const patientDateNaissance = inputs[1] ? inputs[1].value : ''; // Deuxième input = Date de naissance
        const patientAnimal = inputs[2] ? inputs[2].value : 'chien'; // Troisième input = Animal
        const docteurSpan = document.querySelector('.docteur');
        const docteur = docteurSpan ? docteurSpan.textContent.replace('Dr ', '').trim() : '';
        
        // Préparer les données pour l'API
        const maintenant = new Date();
        const data = {
            nom: patientNomPrenom.split(' ').slice(0, -1).join(' ').trim() || patientNomPrenom, // Extraire le nom
            prenom: patientNomPrenom.split(' ').slice(-1).join(' ').trim() || '', // Extraire le prénom
            medecin: docteur,
            classe: "classe03",
            type_de_vaccin: "Cellulaire",
            shema: "Essen",
            date_de_certificat: maintenant.toISOString().split('T')[0], // Date d'aujourd'hui
            date_de_naissance: patientDateNaissance,
            animal: patientAnimal,
            heure_creation: maintenant.toTimeString().split(' ')[0] // Ajouter l'heure pour éviter les doublons
        };
        
        // Envoyer les données à l'API
        fetch('http://localhost:5000/api/ajouter_antirabique', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(data)
        })
        .then(response => response.json())
        .then(result => {
            if (result.success) {
                alert('Certificat antirabique sauvegardé avec succès!');
            } else {
                alert('Erreur lors de la sauvegarde: ' + result.message);
            }
        })
        .catch(error => {
            console.error('Erreur:', error);
            alert('Erreur de connexion au serveur');
        });
    }

    const polycliniqueInput = document.getElementById('polyclinique');
    if (polycliniqueInput) {
        polycliniqueInput.addEventListener('input', function () {
            localStorage.setItem('polyclinique', this.value);
        });
    }

    const polycliniqueArInput = document.getElementById('polyclinique-ar');
    if (polycliniqueArInput) {
        polycliniqueArInput.addEventListener('input', function () {
            localStorage.setItem('polyclinique-ar', this.value);
        });
    }

    document.getElementById('printButton').addEventListener('click', function () {
        window.print();
    });
    
    document.getElementById('saveButtonessen3').addEventListener('click', function () {
        sauvegarderCertificatAntirabique();
    });
    </script> 
    </body>
    </html>
    `;

    var newWindow = window.open("", "_blank");
    if (newWindow) {
        newWindow.document.write(certificatContent);
        newWindow.document.close();
    } else {
        console.log("Popup bloquée par le navigateur.");
    }
}

function vaccint3(dateMorsure, poidsInput) {
    // Récupérer les informations du patient depuis le formulaire
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '';
    const patientAge = document.getElementById('patientAge').value || '';
    const patientDateNaissance = document.getElementById('patientDateNaissance').value || '';
    
    // Utiliser la date de naissance si disponible, sinon l'âge
    let ageInfo = '';
    if (patientDateNaissance) {
        ageInfo = patientDateNaissance;
    } else if (patientAge) {
        ageInfo = patientAge;
    } else {
        ageInfo = '[Date de naissance]';
    }
    
    // Calculer le SAR (Sérum Antirabique) en fonction du poids
    const poids = parseFloat(poidsInput) || 0;
    let sar = poids / 5;
    if (poids >= 75) {
        sar = 15;
    }
    sar = Math.round(sar * 100) / 100;
    
    // Calculer les dates pour le schéma Tissulaire avec SAR (J0, J3, J7, J14, J24,J34, J90)


  

    const dateJour0 = new Date(dateMorsure);
    const dateJour1 = new Date(dateJour0);
    dateJour1.setDate(dateJour0.getDate() + 1);
    const dateJour2 = new Date(dateJour0);
    dateJour2.setDate(dateJour0.getDate() + 2);
    const dateJour3 = new Date(dateJour0);
    dateJour3.setDate(dateJour0.getDate() + 3);
    const dateJour4 = new Date(dateJour0);
    dateJour4.setDate(dateJour0.getDate() + 4);
    const dateJour5 = new Date(dateJour0);
    dateJour5.setDate(dateJour0.getDate() + 5);
    const dateJour6 = new Date(dateJour0);
    dateJour6.setDate(dateJour0.getDate() + 6);
    const dateJour10 = new Date(dateJour0);
    dateJour10.setDate(dateJour0.getDate() + 10);
    const dateJour14 = new Date(dateJour0);
    dateJour14.setDate(dateJour0.getDate() + 14);
    const dateJour24 = new Date(dateJour0);
    dateJour24.setDate(dateJour0.getDate() + 24);
    const dateJour34 = new Date(dateJour0);
    dateJour34.setDate(dateJour0.getDate() + 34);
    const dateJour90 = new Date(dateJour0);
    dateJour90.setDate(dateJour0.getDate() + 90);
	
	 // Formatage des dates
    const formatDate = (date) => `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;


    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";
    const docteur = localStorage.getItem('docteur') || "";
    const animal = "chien"; // Animal en cause (valeur par défaut)

    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';

    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"></div>';
    }

    // Générer le certificat Vaccin T Schema 3
    const certificatContent = `
    <!DOCTYPE html>
    <html lang="fr">
    <head>
    <meta charset="UTF-8">
    <title>Certificat Antirabique - Vaccin T Schema 3 (Tissulaire avec SAR)</title>
    <style>
    body {
        font-family: Arial, sans-serif;
        padding: 20px;
        background-color: #f9f9f9;
    }
    .certificat {
        background-color: white;
        border: 1px solid #ddd;
        padding: 20px;
        max-width: 600px;
        margin: 0 auto;
        margin-top: 60px;
        box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
    }
    h1 {
        text-align: center;
        color: #333;
        text-decoration: underline;
        font-size: 16px;
    }
    h2 {
        text-align: center;
        color: #555;
        font-size: 10px;
        margin-top: 5px;
        margin-bottom: 15px;
    }
    p {
        line-height: 1.5;
        color: #555;
    }
    .small-text {
        font-size: 12px;
    }
    .print-button {
        text-align: center;
        margin-top: 20px;
    }
    .print-button button {
        padding: 10px 20px;
        font-size: 16px;
        background-color: #007bff;
        color: white;
        border: none;
        border-radius: 5px;
        cursor: pointer;
    }
    .print-button button:hover {
        background-color: #0056b3;
    }
    @media print {
        @page {
            size: A5;
            margin: 0.2cm 0.2cm 0.2cm 0.2cm;
        }
        body {
            margin: 0 !important;
            padding: 0 !important;
            font-size: 10pt !important;
            background-color: white;
        }
        .certificat {
            padding: 2px 8px !important;
            max-width: 100% !important;
            border: none;
            box-shadow: none;
            margin-top: 0;
        }
        h1 {
            font-size: 14pt !important;
            margin: 5px 0 !important;
            margin-top: 2cm !important;
        }
        h2 {
            font-size: 12pt !important;
            margin: 3px 0 !important;
        }
        p {
            font-size: 9pt !important;
            margin: 2px 0 !important;
            line-height: 1.2 !important;
        }
        input[type="text"],
        input[type="date"],
        textarea {
            border: none !important;
            background: none !important;
            box-shadow: none !important;
            outline: none !important;
            font-size: 9pt !important;
        }
        input[type="text"]:focus,
        input[type="date"]:focus,
        textarea:focus {
            border: none !important;
            outline: none !important;
        }
        .print-button {
            display: none;
        }
        .docteur {
            font-weight: bold;
            font-size: 14pt !important;
            margin-right: 50px;
        }
        /* Additional space optimization */
        * {
            margin-top: 0 !important;
            margin-bottom: 2px !important;
        }
    }
    </style>
    </head>
    <body>
   ${enteteContent}

    <div class="certificat">
        <h1>Schéma Antirabique - Tissulaire avec SAR</h1>
        <h2>Selon l'instruction N16 du 15 Juillet 2024 relative à la conduite à tenir devant un risque rabique</h2>
        <p>
        NOM : <strong><input type="text" value="${patientNomPrenom}" style="width: auto;"></strong><br>
        Date de naissance : <strong><input type="text" value="${ageInfo}" style="width: auto;"></strong><br>
        Animal en cause : <strong><input type="text" value="${animal}" style="width: auto;"></strong><br>
        Poids : <strong><input type="text" value="${poidsInput} kg" style="width: auto;"></strong><br>
        SAR : <strong><input type="text" value="${sar} cc" style="width: auto;"></strong><br>
		
        </p>
        <p>
         <br>
          <span style="font-size: smaller;">Classe 03, schéma choisi : vaccin tissulaire / avec SAR</span><br><br>
        
Jour 0 : <input type="date" id="dateJour0" value="${formatDate(dateJour0)}" readonly> <span class="small-text">( les 07 premiers jours les inj s/cutanée péri-ombilicale)</span><br>
Jour 1 : <input type="date" id="dateJour1" value="${formatDate(dateJour1)}" readonly><br>
Jour 2 : <input type="date" id="dateJour2" value="${formatDate(dateJour2)}" readonly><br>
Jour 3 : <input type="date" id="dateJour3" value="${formatDate(dateJour3)}" readonly><br>
Jour 4 : <input type="date" id="dateJour4" value="${formatDate(dateJour4)}" readonly><br>
Jour 5 : <input type="date" id="dateJour5" value="${formatDate(dateJour5)}" readonly><br>
Jour 6 : <input type="date" id="dateJour6" value="${formatDate(dateJour6)}" readonly><br>
=========== les rappeles en ID dans dans les av bras=====<br>
Jour 10 : <input type="date" id="dateJour10" value="${formatDate(dateJour10)}" readonly><br>
Jour 14 : <input type="date" id="dateJour14" value="${formatDate(dateJour14)}" readonly><br>
Jour 24 : <input type="date" id="dateJour24" value="${formatDate(dateJour24)}" readonly><br>
Jour 34 : <input type="date" id="dateJour34" value="${formatDate(dateJour34)}" readonly><br>
Jour 90 : <input type="date" id="dateJour90" value="${formatDate(dateJour90)}" readonly><br>
<span class="small-text">en cas d'âge <05ans la dose sera 1/2 amp( 01 ml)</span><br>
        </p>
        <p style="text-align: right; margin-top: 30px;">
        Medecin traitant <br>
        <span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>&nbsp&nbsp&nbsp&nbsp;
        </p>
    </div>

    <div class="print-button">
        <button id="printButton">Imprimer le schéma</button>
		<button id="saveButtonT3">Sauvegarder</button>
    </div>
 

    <script>
    function sauvegarderCertificatAntirabique() {
        // Récupérer les informations du patient depuis les champs du certificat affiché
        const inputs = document.querySelectorAll('input[type="text"]');
        const patientNomPrenom = inputs[0] ? inputs[0].value : ''; // Premier input = NOM
        const patientDateNaissance = inputs[1] ? inputs[1].value : ''; // Deuxième input = Date de naissance
        const patientAnimal = inputs[2] ? inputs[2].value : 'chien'; // Troisième input = Animal
        const docteurSpan = document.querySelector('.docteur');
        const docteur = docteurSpan ? docteurSpan.textContent.replace('Dr ', '').trim() : '';
        
        // Préparer les données pour l'API
        const maintenant = new Date();
        const data = {
            nom: patientNomPrenom.split(' ').slice(0, -1).join(' ').trim() || patientNomPrenom, // Extraire le nom
            prenom: patientNomPrenom.split(' ').slice(-1).join(' ').trim() || '', // Extraire le prénom
            medecin: docteur,
            classe: "classe03",
            type_de_vaccin: "Tissulaire",
            shema: "avec SAR",
            date_de_certificat: maintenant.toISOString().split('T')[0], // Date d'aujourd'hui
            date_de_naissance: patientDateNaissance,
            animal: patientAnimal,
            heure_creation: maintenant.toTimeString().split(' ')[0] // Ajouter l'heure pour éviter les doublons
        };
        
        // Envoyer les données à l'API
        fetch('http://localhost:5000/api/ajouter_antirabique', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(data)
        })
        .then(response => response.json())
        .then(result => {
            if (result.success) {
                alert('Certificat antirabique sauvegardé avec succès!');
            } else {
                alert('Erreur lors de la sauvegarde: ' + result.message);
            }
        })
        .catch(error => {
            console.error('Erreur:', error);
            alert('Erreur de connexion au serveur');
        });
    }

    const polycliniqueInput = document.getElementById('polyclinique');
    if (polycliniqueInput) {
        polycliniqueInput.addEventListener('input', function () {
            localStorage.setItem('polyclinique', this.value);
        });
    }

    const polycliniqueArInput = document.getElementById('polyclinique-ar');
    if (polycliniqueArInput) {
        polycliniqueArInput.addEventListener('input', function () {
            localStorage.setItem('polyclinique-ar', this.value);
        });
    }

    document.getElementById('printButton').addEventListener('click', function () {
        window.print();
    });
    
    document.getElementById('saveButtonT3').addEventListener('click', function () {
        sauvegarderCertificatAntirabique();
    });
    </script> 
    </body>
    </html>
    `;

    var newWindow = window.open("", "_blank");
    if (newWindow) {
        newWindow.document.write(certificatContent);
        newWindow.document.close();
    } else {
        console.log("Popup bloquée par le navigateur.");
    }
}

function risqueHemorragique3(dateMorsure, poidsInput) {
    // Récupérer les informations du patient depuis le formulaire
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '';
    const patientAge = document.getElementById('patientAge').value || '';
    const patientDateNaissance = document.getElementById('patientDateNaissance').value || '';
    
    // Utiliser la date de naissance si disponible, sinon l'âge
    let ageInfo = '';
    if (patientDateNaissance) {
        ageInfo = patientDateNaissance;
    } else if (patientAge) {
        ageInfo = patientAge;
    } else {
        ageInfo = '[Date de naissance]';
    }
    
    // Calculer le SAR (Sérum Antirabique) en fonction du poids
    const poids = parseFloat(poidsInput) || 0;
    let sar = poids / 5;
    if (poids >= 75) {
        sar = 15;
    }
    sar = Math.round(sar * 100) / 100;
    
    // Calculer les dates pour le schéma Risque Hémorragique (J0, J3, J7)
    const dateJour0 = new Date(dateMorsure);
    const dateJour3 = new Date(dateJour0);
    dateJour3.setDate(dateJour0.getDate() + 3);
    const dateJour7 = new Date(dateJour0);
    dateJour7.setDate(dateJour0.getDate() + 7);


    // Fonction pour formater la date
    const formatDate = (date) => `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;

    const dateFormattedJour0 = formatDate(dateJour0);
    const dateFormattedJour3 = formatDate(dateJour3);
    const dateFormattedJour7 = formatDate(dateJour7);
  

    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";
    const docteur = localStorage.getItem('docteur') || "";
    const animal = "chien"; // Animal en cause (valeur par défaut)

    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';

    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"></div>';
    }

    // Générer le certificat Risque Hémorragique 3
    const certificatContent = `
    <!DOCTYPE html>
    <html lang="fr">
    <head>
    <meta charset="UTF-8">
    <title>Certificat Antirabique - Risque Hémorragique 3</title>
    <style>
    body {
        font-family: Arial, sans-serif;
        padding: 20px;
        background-color: #f9f9f9;
    }
    .certificat {
        background-color: white;
        border: 1px solid #ddd;
        padding: 20px;
        max-width: 600px;
        margin: 0 auto;
        margin-top: 60px;
        box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
    }
    h1 {
        text-align: center;
        color: #333;
        text-decoration: underline;
        font-size: 16px;
    }
    h2 {
        text-align: center;
        color: #555;
        font-size: 10px;
        margin-top: 5px;
        margin-bottom: 15px;
    }
    p {
        line-height: 1.5;
        color: #555;
    }
    .small-text {
        font-size: 12px;
    }
    .print-button {
        text-align: center;
        margin-top: 20px;
    }
    .print-button button {
        padding: 10px 20px;
        font-size: 16px;
        background-color: #007bff;
        color: white;
        border: none;
        border-radius: 5px;
        cursor: pointer;
    }
    .print-button button:hover {
        background-color: #0056b3;
    }
    @media print {
        @page {
            size: A5;
            margin: 0.2cm 0.2cm 0.2cm 0.2cm;
        }
        body {
            margin: 0 !important;
            padding: 0 !important;
            font-size: 10pt !important;
            background-color: white;
        }
        .certificat {
            padding: 2px 8px !important;
            max-width: 100% !important;
            border: none;
            box-shadow: none;
            margin-top: 0;
        }
        h1 {
            font-size: 14pt !important;
            margin: 5px 0 !important;
            margin-top: 2cm !important;
        }
        h2 {
            font-size: 12pt !important;
            margin: 3px 0 !important;
        }
        p {
            font-size: 9pt !important;
            margin: 2px 0 !important;
            line-height: 1.2 !important;
        }
        input[type="text"],
        input[type="date"],
        textarea {
            border: none !important;
            background: none !important;
            box-shadow: none !important;
            outline: none !important;
            font-size: 9pt !important;
        }
        input[type="text"]:focus,
        input[type="date"]:focus,
        textarea:focus {
            border: none !important;
            outline: none !important;
        }
        .print-button {
            display: none;
        }
        .docteur {
            font-weight: bold;
            font-size: 14pt !important;
            margin-right: 50px;
        }
        /* Additional space optimization */
        * {
            margin-top: 0 !important;
            margin-bottom: 2px !important;
        }
    }
    </style>
    </head>
    <body>
   ${enteteContent}

    <div class="certificat">
        <h1>Schéma Antirabique - Risque Hémorragique/Qte Limitées (Voie Intradermique)</h1>
        <h2>Selon l'instruction N16 du 15 Juillet 2024 relative à la conduite à tenir devant un risque rabique</h2>
        <p>
        NOM : <strong><input type="text" value="${patientNomPrenom}" style="width: auto;"></strong><br>
        Date de naissance : <strong><input type="text" value="${ageInfo}" style="width: auto;"></strong><br>
        Animal en cause : <strong><input type="text" value="${animal}" style="width: auto;"></strong><br>
        Poids : <strong><input type="text" value="${poidsInput} kg" style="width: auto;"></strong><br>
        SAR : <strong><input type="text" value="${sar} cc" style="width: auto;"></strong><br>
        </p>
        <p>
         <br>
         <span class="small-text">Schéma adapté pour patient à  risque hémorragique ou quantités limitées</span>
		<br>
        </p>
        <p>
         <strong>Personne sans antécédent de vaccination:</strong><br>
         Un total de six doses : 2 doses en deux [2] sites différents<br><br>
         Jour 0 : <input type="date" id="dateJour0" value="${dateFormattedJour0}" readonly> ( 2 doses en ID + SAR )<br>
         Jour 3 : <input type="date" id="dateJour3" value="${dateFormattedJour3}" readonly> ( 2 doses en ID )<br>
         Jour 7 : <input type="date" id="dateJour7" value="${dateFormattedJour7}" readonly> ( 2 doses en ID ))<br>
        
         <br><br>
       NB: 01 dose =0.1ml
        </p>
        <p style="text-align: right; margin-top: 30px;">
        Medecin traitant <br>
        <span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>&nbsp&nbsp&nbsp&nbsp;
        </p>
    </div>

    <div class="print-button">
        <button id="printButton">Imprimer le schéma</button>
		<button id="saveButtonRh">Sauvegarder</button>
    </div>
 
    <script>
    function sauvegarderCertificatAntirabique() {
        // Récupérer les informations du patient depuis les champs du certificat affiché
        const inputs = document.querySelectorAll('input[type="text"]');
        const patientNomPrenom = inputs[0] ? inputs[0].value : ''; // Premier input = NOM
        const patientDateNaissance = inputs[1] ? inputs[1].value : ''; // Deuxième input = Date de naissance
        const patientAnimal = inputs[2] ? inputs[2].value : 'chien'; // Troisième input = Animal
        const docteurSpan = document.querySelector('.docteur');
        const docteur = docteurSpan ? docteurSpan.textContent.replace('Dr ', '').trim() : '';
        
        // Préparer les données pour l'API
        const maintenant = new Date();
        const data = {
            nom: patientNomPrenom.split(' ').slice(0, -1).join(' ').trim() || patientNomPrenom, // Extraire le nom
            prenom: patientNomPrenom.split(' ').slice(-1).join(' ').trim() || '', // Extraire le prénom
            medecin: docteur,
            classe: "classe03",
            type_de_vaccin: "Cellulaire",
            shema: "risqueHemorragique3",
            date_de_certificat: maintenant.toISOString().split('T')[0], // Date d'aujourd'hui
            date_de_naissance: patientDateNaissance,
            animal: patientAnimal,
            heure_creation: maintenant.toTimeString().split(' ')[0] // Ajouter l'heure pour éviter les doublons
        };
        
        // Envoyer les données à l'API
        fetch('http://localhost:5000/api/ajouter_antirabique', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(data)
        })
        .then(response => response.json())
        .then(result => {
            if (result.success) {
                alert('Certificat antirabique sauvegardé avec succès!');
            } else {
                alert('Erreur lors de la sauvegarde: ' + result.message);
            }
        })
        .catch(error => {
            console.error('Erreur:', error);
            alert('Erreur de connexion au serveur');
        });
    }

    const polycliniqueInput = document.getElementById('polyclinique');
    if (polycliniqueInput) {
        polycliniqueInput.addEventListener('input', function () {
            localStorage.setItem('polyclinique', this.value);
        });
    }

    const polycliniqueArInput = document.getElementById('polyclinique-ar');
    if (polycliniqueArInput) {
        polycliniqueArInput.addEventListener('input', function () {
            localStorage.setItem('polyclinique-ar', this.value);
        });
    }

    document.getElementById('printButton').addEventListener('click', function () {
        window.print();
    });
    
    document.getElementById('saveButtonRh').addEventListener('click', function () {
        sauvegarderCertificatAntirabique();
    });
    </script> 
    </body>
    </html>
    `;

    var newWindow = window.open("", "_blank");
    if (newWindow) {
        newWindow.document.write(certificatContent);
        newWindow.document.close();
    } else {
        console.log("Popup bloquée par le navigateur.");
    }
}

function prophylaxiePreExpositionSchema1Classe3(dateMorsure, poidsInput) {
    // Récupérer les informations du patient depuis le formulaire
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '';
    const patientAge = document.getElementById('patientAge').value || '';
    const patientDateNaissance = document.getElementById('patientDateNaissance').value || '';
    
    // Utiliser la date de naissance si disponible, sinon l'âge
    let ageInfo = '';
    if (patientDateNaissance) {
        ageInfo = patientDateNaissance;
    } else if (patientAge) {
        ageInfo = patientAge;
    } else {
        ageInfo = '[Date de naissance]';
    }
    
    // Calculer le SAR (Sérum Antirabique) en fonction du poids
    const poids = parseFloat(poidsInput) || 0;
    let sar = poids / 5;
    if (poids >= 75) {
        sar = 15;
    }
    sar = Math.round(sar * 100) / 100;
    
    // Calculer les dates pour le schéma ATCD Vaccinaux Schéma 1 (J0, J3, J7, J14)
    const dateJour0 = new Date(dateMorsure);
    const datePlus3 = new Date(dateJour0);
    datePlus3.setDate(dateJour0.getDate() + 3);
    const datePlus7 = new Date(dateJour0);
    datePlus7.setDate(dateJour0.getDate() + 7);
    const datePlus14 = new Date(dateJour0);
    datePlus14.setDate(dateJour0.getDate() + 14);

    const dateFormattedJour0 = `${dateJour0.getFullYear()}-${String(dateJour0.getMonth() + 1).padStart(2, '0')}-${String(dateJour0.getDate()).padStart(2, '0')}`;
    const dateFormattedPlus3 = `${datePlus3.getFullYear()}-${String(datePlus3.getMonth() + 1).padStart(2, '0')}-${String(datePlus3.getDate()).padStart(2, '0')}`;
    const dateFormattedPlus7 = `${datePlus7.getFullYear()}-${String(datePlus7.getMonth() + 1).padStart(2, '0')}-${String(datePlus7.getDate()).padStart(2, '0')}`;
    const dateFormattedPlus14 = `${datePlus14.getFullYear()}-${String(datePlus14.getMonth() + 1).padStart(2, '0')}-${String(datePlus14.getDate()).padStart(2, '0')}`;

    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";
    const docteur = localStorage.getItem('docteur') || "";
    const animal = "chien"; // Animal en cause (valeur par défaut)

    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';

    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"></div>';
    }

    // Générer le certificat ATCD Vaccinaux Schéma 1
    const certificatContent = `
    <!DOCTYPE html>
    <html lang="fr">
    <head>
    <meta charset="UTF-8">
    <title>Certificat Antirabique - ATCD Vaccinaux Schéma 1 (Classe 3)</title>
    <style>
    body {
        font-family: Arial, sans-serif;
        padding: 20px;
        background-color: #f9f9f9;
    }
    .certificat {
        background-color: white;
        border: 1px solid #ddd;
        padding: 20px;
        max-width: 600px;
        margin: 0 auto;
        margin-top: 60px;
        box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
    }
    h1 {
        text-align: center;
        color: #333;
        text-decoration: underline;
        font-size: 16px;
    }
    h2 {
        text-align: center;
        color: #555;
        font-size: 10px;
        margin-top: 5px;
        margin-bottom: 15px;
    }
    p {
        line-height: 1.5;
        color: #555;
    }
    .small-text {
        font-size: 12px;
    }
    .print-button {
        text-align: center;
        margin-top: 20px;
    }
    .print-button button {
        padding: 10px 20px;
        font-size: 16px;
        background-color: #007bff;
        color: white;
        border: none;
        border-radius: 5px;
        cursor: pointer;
    }
    .print-button button:hover {
        background-color: #0056b3;
    }
    @media print {
        @page {
            size: A5;
            margin: 0.2cm 0.2cm 0.2cm 0.2cm;
        }
        body {
            margin: 0 !important;
            padding: 0 !important;
            font-size: 10pt !important;
            background-color: white;
        }
        .certificat {
            padding: 2px 8px !important;
            max-width: 100% !important;
            border: none;
            box-shadow: none;
            margin-top: 0;
        }
        h1 {
            font-size: 14pt !important;
            margin: 5px 0 !important;
            margin-top: 2cm !important;
        }
        h2 {
            font-size: 12pt !important;
            margin: 3px 0 !important;
        }
        p {
            font-size: 9pt !important;
            margin: 2px 0 !important;
            line-height: 1.2 !important;
        }
        input[type="text"],
        input[type="date"],
        textarea {
            border: none !important;
            background: none !important;
            box-shadow: none !important;
            outline: none !important;
            font-size: 9pt !important;
        }
        input[type="text"]:focus,
        input[type="date"]:focus,
        textarea:focus {
            border: none !important;
            outline: none !important;
        }
        .print-button {
            display: none;
        }
        .docteur {
            font-weight: bold;
            font-size: 14pt !important;
            margin-right: 50px;
        }
        /* Additional space optimization */
        * {
            margin-top: 0 !important;
            margin-bottom: 2px !important;
        }
    }
    </style>
    </head>
    <body>
   ${enteteContent}

    <div class="certificat">
        <h1>Schéma Antirabique -  Avec ATCD Vaccinaux(Schéma 1</h1>
        <h2>Selon l'instruction N16 du 15 Juillet 2024 relative à la conduite à tenir devant un risque rabique</h2>
        <p>
        NOM : <strong><input type="text" value="${patientNomPrenom}" style="width: auto;"></strong><br>
        Date de naissance : <strong><input type="text" value="${ageInfo}" style="width: auto;"></strong><br>
        Animal en cause : <strong><input type="text" value="${animal}" style="width: auto;"></strong><br>
        Poids : <strong><input type="text" value="${poidsInput} kg" style="width: auto;"></strong><br>
		<br>
       Classe 03, schéma choisi : Avec ATCD Vaccinaux (Schéma 1)<br>
       <br>
        </p>
        <p>
         <strong style="font-size: 14px;">Personne ayant reçu une prophylaxie Pré-exPosition, ou (>= 02 doses):</strong><br>
         <br><br>
         Schéma 1 : Un total de deux (2) doses<br><br>
         Jour 0 : <input type="date" id="dateJour0" value="${dateFormattedJour0}" readonly> (une (1) dose ID en un seul site)<br>
         Jour 3 : <input type="date" id="datePlus3" value="${dateFormattedPlus3}" readonly> (une (1) dose ID en un seul site)<br>
        
         <br><br>
        NB:01 dose=0,1mI
        </p>
        <p style="text-align: right; margin-top: 30px;">
        Medecin traitant <br>
        <span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>&nbsp&nbsp&nbsp&nbsp&nbsp;
        </p>
    </div>

    <div class="print-button">
        <button id="printButton">Imprimer le schéma</button>
		<button id="saveButtonAtcd1">Sauvegarder</button>
    </div>
    

    <script>
    function sauvegarderCertificatAntirabique() {
        // Récupérer les informations du patient depuis les champs du certificat affiché
        const inputs = document.querySelectorAll('input[type="text"]');
        const patientNomPrenom = inputs[0] ? inputs[0].value : ''; // Premier input = NOM
        const patientDateNaissance = inputs[1] ? inputs[1].value : ''; // Deuxième input = Date de naissance
        const patientAnimal = inputs[2] ? inputs[2].value : 'chien'; // Troisième input = Animal
        const docteurSpan = document.querySelector('.docteur');
        const docteur = docteurSpan ? docteurSpan.textContent.replace('Dr ', '').trim() : '';
        
        // Préparer les données pour l'API
        const maintenant = new Date();
        const data = {
            nom: patientNomPrenom.split(' ').slice(0, -1).join(' ').trim() || patientNomPrenom, // Extraire le nom
            prenom: patientNomPrenom.split(' ').slice(-1).join(' ').trim() || '', // Extraire le prénom
            medecin: docteur,
            classe: "classe03",
            type_de_vaccin: "Cellulaire",
            shema: "avec ATCD 1",
            date_de_certificat: maintenant.toISOString().split('T')[0], // Date d'aujourd'hui
            date_de_naissance: patientDateNaissance,
            animal: patientAnimal,
            heure_creation: maintenant.toTimeString().split(' ')[0] // Ajouter l'heure pour éviter les doublons
        };
        
        // Envoyer les données à l'API
        fetch('http://localhost:5000/api/ajouter_antirabique', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(data)
        })
        .then(response => response.json())
        .then(result => {
            if (result.success) {
                alert('Certificat antirabique sauvegardé avec succès!');
            } else {
                alert('Erreur lors de la sauvegarde: ' + result.message);
            }
        })
        .catch(error => {
            console.error('Erreur:', error);
            alert('Erreur de connexion au serveur');
        });
    }

    const polycliniqueInput = document.getElementById('polyclinique');
    if (polycliniqueInput) {
        polycliniqueInput.addEventListener('input', function () {
            localStorage.setItem('polyclinique', this.value);
        });
    }

    const polycliniqueArInput = document.getElementById('polyclinique-ar');
    if (polycliniqueArInput) {
        polycliniqueArInput.addEventListener('input', function () {
            localStorage.setItem('polyclinique-ar', this.value);
        });
    }

    document.getElementById('printButton').addEventListener('click', function () {
        window.print();
    });
    
    document.getElementById('saveButtonAtcd1').addEventListener('click', function () {
        sauvegarderCertificatAntirabique();
    });
    </script> 
    </body>
    </html>
    `;

    var newWindow = window.open("", "_blank");
    if (newWindow) {
        newWindow.document.write(certificatContent);
        newWindow.document.close();
    } else {
        console.log("Popup bloquée par le navigateur.");
    }
}

function prophylaxiePreExpositionSchema2Classe3(dateMorsure, poidsInput) {
    // Récupérer les informations du patient depuis le formulaire
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '';
    const patientAge = document.getElementById('patientAge').value || '';
    const patientDateNaissance = document.getElementById('patientDateNaissance').value || '';
    
    // Utiliser la date de naissance si disponible, sinon l'âge
    let ageInfo = '';
    if (patientDateNaissance) {
        ageInfo = patientDateNaissance;
    } else if (patientAge) {
        ageInfo = patientAge;
    } else {
        ageInfo = '[Date de naissance]';
    }
    
    // Calculer le SAR (Sérum Antirabique) en fonction du poids
    const poids = parseFloat(poidsInput) || 0;
    let sar = poids / 5;
    if (poids >= 75) {
        sar = 15;
    }
    sar = Math.round(sar * 100) / 100;
    
    // Calculer les dates pour le schéma ATCD Vaccinaux Schéma 2 (J0 uniquement)
    const dateJour0 = new Date(dateMorsure);

    const dateFormattedJour0 = `${dateJour0.getFullYear()}-${String(dateJour0.getMonth() + 1).padStart(2, '0')}-${String(dateJour0.getDate()).padStart(2, '0')}`;

    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";
    const docteur = localStorage.getItem('docteur') || "";
    const animal = "chien"; // Animal en cause (valeur par défaut)

    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';

    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"></div>';
    }

    // Générer le certificat ATCD Vaccinaux Schéma 2
    const certificatContent = `
    <!DOCTYPE html>
    <html lang="fr">
    <head>
    <meta charset="UTF-8">
    <title>Certificat Antirabique - ATCD Vaccinaux Schéma 2 (Classe 3)</title>
    <style>
    body {
        font-family: Arial, sans-serif;
        padding: 20px;
        background-color: #f9f9f9;
    }
    .certificat {
        background-color: white;
        border: 1px solid #ddd;
        padding: 20px;
        max-width: 600px;
        margin: 0 auto;
        margin-top: 60px;
        box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
    }
    h1 {
        text-align: center;
        color: #333;
        text-decoration: underline;
        font-size: 16px;
    }
    h2 {
        text-align: center;
        color: #555;
        font-size: 10px;
        margin-top: 5px;
        margin-bottom: 15px;
    }
    p {
        line-height: 1.5;
        color: #555;
    }
    .small-text {
        font-size: 12px;
    }
    .print-button {
        text-align: center;
        margin-top: 20px;
    }
    .print-button button {
        padding: 10px 20px;
        font-size: 16px;
        background-color: #007bff;
        color: white;
        border: none;
        border-radius: 5px;
        cursor: pointer;
    }
    .print-button button:hover {
        background-color: #0056b3;
    }
    @media print {
        @page {
            size: A5;
            margin: 0.2cm 0.2cm 0.2cm 0.2cm;
        }
        body {
            margin: 0 !important;
            padding: 0 !important;
            font-size: 10pt !important;
            background-color: white;
        }
        .certificat {
            padding: 2px 8px !important;
            max-width: 100% !important;
            border: none;
            box-shadow: none;
            margin-top: 0;
        }
        h1 {
            font-size: 14pt !important;
            margin: 5px 0 !important;
            margin-top: 2cm !important;
        }
        h2 {
            font-size: 12pt !important;
            margin: 3px 0 !important;
        }
        p {
            font-size: 9pt !important;
            margin: 2px 0 !important;
            line-height: 1.2 !important;
        }
        input[type="text"],
        input[type="date"],
        textarea {
            border: none !important;
            background: none !important;
            box-shadow: none !important;
            outline: none !important;
            font-size: 9pt !important;
        }
        input[type="text"]:focus,
        input[type="date"]:focus,
        textarea:focus {
            border: none !important;
            outline: none !important;
        }
        .print-button {
            display: none;
        }
        .docteur {
            font-weight: bold;
            font-size: 14pt !important;
            margin-right: 50px;
        }
        /* Additional space optimization */
        * {
            margin-top: 0 !important;
            margin-bottom: 2px !important;
        }
    }
    </style>
    </head>
    <body>
   ${enteteContent}

    <div class="certificat">
        <h1>Schéma Antirabique - ATCD Vaccinaux Schéma 2 </h1>
        <h2>Selon l'instruction N16 du 15 Juillet 2024 relative à la conduite à tenir devant un risque rabique</h2>
        <p>
        NOM : <strong><input type="text" value="${patientNomPrenom}" style="width: auto;"></strong><br>
        Date de naissance : <strong><input type="text" value="${ageInfo}" style="width: auto;"></strong><br>
        Animal en cause : <strong><input type="text" value="${animal}" style="width: auto;"></strong><br>
        Poids : <strong><input type="text" value="${poidsInput} kg" style="width: auto;"></strong><br>
        </p>
        <p>
         <strong>Personne ayant reçu une prophylaxie Pré-exPosition, ou (≥ 02 doses):</strong><br>
         Schéma 2 : Un total de quatre (4) doses<br><br>
         Jour 0 : <input type="date" id="dateJour0" value="${dateFormattedJour0}" readonly> (une dose en ID en quatre (4) sites différents)<br>
         <br><br>
        NB: 01 dose=0,1mI
        </p>
        <p style="text-align: right; margin-top: 30px;">
        Medecin traitant <br>
        <span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>&nbsp&nbsp&nbsp&nbsp;
        </p>
    </div>

    <div class="print-button">
        <button id="printButton">Imprimer le schéma</button>
		<button id="saveButtonAtcd2">Sauvegarder</button>
    </div>
    

    <script>
    function sauvegarderCertificatAntirabique() {
        // Récupérer les informations du patient depuis les champs du certificat affiché
        const inputs = document.querySelectorAll('input[type="text"]');
        const patientNomPrenom = inputs[0] ? inputs[0].value : ''; // Premier input = NOM
        const patientDateNaissance = inputs[1] ? inputs[1].value : ''; // Deuxième input = Date de naissance
        const patientAnimal = inputs[2] ? inputs[2].value : 'chien'; // Troisième input = Animal
        const docteurSpan = document.querySelector('.docteur');
        const docteur = docteurSpan ? docteurSpan.textContent.replace('Dr ', '').trim() : '';
        
        // Préparer les données pour l'API
        const maintenant = new Date();
        const data = {
            nom: patientNomPrenom.split(' ').slice(0, -1).join(' ').trim() || patientNomPrenom, // Extraire le nom
            prenom: patientNomPrenom.split(' ').slice(-1).join(' ').trim() || '', // Extraire le prénom
            medecin: docteur,
            classe: "classe03",
            type_de_vaccin: "Cellulaire",
            shema: "avec ATCD 2",
            date_de_certificat: maintenant.toISOString().split('T')[0], // Date d'aujourd'hui
            date_de_naissance: patientDateNaissance,
            animal: patientAnimal,
            heure_creation: maintenant.toTimeString().split(' ')[0] // Ajouter l'heure pour éviter les doublons
        };
        
        // Envoyer les données à l'API
        fetch('http://localhost:5000/api/ajouter_antirabique', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(data)
        })
        .then(response => response.json())
        .then(result => {
            if (result.success) {
                alert('Certificat antirabique sauvegardé avec succès!');
            } else {
                alert('Erreur lors de la sauvegarde: ' + result.message);
            }
        })
        .catch(error => {
            console.error('Erreur:', error);
            alert('Erreur de connexion au serveur');
        });
    }

    const polycliniqueInput = document.getElementById('polyclinique');
    if (polycliniqueInput) {
        polycliniqueInput.addEventListener('input', function () {
            localStorage.setItem('polyclinique', this.value);
        });
    }

    const polycliniqueArInput = document.getElementById('polyclinique-ar');
    if (polycliniqueArInput) {
        polycliniqueArInput.addEventListener('input', function () {
            localStorage.setItem('polyclinique-ar', this.value);
        });
    }

    document.getElementById('printButton').addEventListener('click', function () {
        window.print();
    });
    
    document.getElementById('saveButtonAtcd2').addEventListener('click', function () {
        sauvegarderCertificatAntirabique();
    });
    </script> 
    </body>
    </html>
    `;

    var newWindow = window.open("", "_blank");
    if (newWindow) {
        newWindow.document.write(certificatContent);
        newWindow.document.close();
    } else {
        console.log("Popup bloquée par le navigateur.");
    }
}

function genererCertificatProphylaxieImmunocompetent(dateDebut) {
    // Récupérer les informations du patient depuis le formulaire
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '';
    const patientAge = document.getElementById('patientAge').value || '';
    const patientDateNaissance = document.getElementById('patientDateNaissance').value || '';
    
    // Utiliser la date de naissance si disponible, sinon l'âge
    let ageInfo = '';
    if (patientDateNaissance) {
        ageInfo = patientDateNaissance;
    } else if (patientAge) {
        ageInfo = patientAge;
    } else {
        ageInfo = '[Date de naissance]';
    }
    
    // Calculer les dates pour la prophylaxie pré-exposition immunocompétent
    const dateJour0 = new Date(dateDebut);
    const datePlus7 = new Date(dateJour0);
    datePlus7.setDate(dateJour0.getDate() + 7);
    
    // Ajouter 12 mois (365 jours) pour le rappel
    const datePlus12Mois = new Date(dateJour0);
    datePlus12Mois.setFullYear(dateJour0.getFullYear() + 1);

    const dateFormattedJour0 = `${dateJour0.getFullYear()}-${String(dateJour0.getMonth() + 1).padStart(2, '0')}-${String(dateJour0.getDate()).padStart(2, '0')}`;
    const dateFormattedPlus7 = `${datePlus7.getFullYear()}-${String(datePlus7.getMonth() + 1).padStart(2, '0')}-${String(datePlus7.getDate()).padStart(2, '0')}`;
    const dateFormattedPlus12Mois = `${datePlus12Mois.getFullYear()}-${String(datePlus12Mois.getMonth() + 1).padStart(2, '0')}-${String(datePlus12Mois.getDate()).padStart(2, '0')}`;

    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";
    const docteur = localStorage.getItem('docteur') || "";

    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';

    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"></div>';
    }

    // Générer le certificat de prophylaxie pré-exposition immunocompétent
    const certificatContent = `
    <!DOCTYPE html>
    <html lang="fr">
    <head>
    <meta charset="UTF-8">
    <title>Prophylaxie Pré-exposition - Patient Immunocompétent</title>
    <style>
    body {
        font-family: Arial, sans-serif;
        padding: 20px;
        background-color: #f9f9f9;
    }
    .certificat {
        background-color: white;
        border: 1px solid #ddd;
        padding: 20px;
        max-width: 600px;
        margin: 0 auto;
        margin-top: 60px;
        box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
    }
    h1 {
        text-align: center;
        color: #333;
        text-decoration: underline;
        font-size: 16px;
    }
    h2 {
        text-align: center;
        color: #555;
        font-size: 10px;
        margin-top: 5px;
        margin-bottom: 15px;
    }
    p {
        line-height: 1.5;
        color: #555;
    }
    .print-button {
        text-align: center;
        margin-top: 20px;
    }
    .print-button button {
        padding: 10px 20px;
        font-size: 16px;
        background-color: #007bff;
        color: white;
        border: none;
        border-radius: 5px;
        cursor: pointer;
    }
    .print-button button:hover {
        background-color: #0056b3;
    }
    @media print {
        @page {
            size: A5;
            margin: 0.2cm 0.2cm 0.2cm 0.2cm;
        }
        body {
            margin: 0 !important;
            padding: 0 !important;
            font-size: 10pt !important;
            background-color: white;
        }
        .certificat {
            padding: 2px 8px !important;
            max-width: 100% !important;
            border: none;
            box-shadow: none;
            margin-top: 0;
        }
        h1 {
            font-size: 14pt !important;
            margin: 5px 0 !important;
            margin-top: 2cm !important;
        }
        h2 {
            font-size: 12pt !important;
            margin: 3px 0 !important;
        }
        p {
            font-size: 9pt !important;
            margin: 2px 0 !important;
            line-height: 1.2 !important;
        }
        input[type="text"],
        input[type="date"],
        textarea {
            border: none !important;
            background: none !important;
            box-shadow: none !important;
            outline: none !important;
            font-size: 9pt !important;
        }
        input[type="text"]:focus,
        input[type="date"]:focus,
        textarea:focus {
            border: none !important;
            outline: none !important;
        }
        .print-button {
            display: none;
        }
        .docteur {
            font-weight: bold;
            font-size: 14pt !important;
            margin-right: 50px;
        }
        /* Additional space optimization */
        * {
            margin-top: 0 !important;
            margin-bottom: 2px !important;
        }
    }
    </style>
    </head>
    <body>
   ${enteteContent}

    <div class="certificat">
        <h1>Prophylaxie Pré-exposition - Patient Immunocompétent</h1>
        <h2>Selon l'instruction N16 du 15 Juillet 2024 relative à la conduite à tenir devant un risque rabique</h2>
        <p>
        NOM : <strong><input type="text" value="${patientNomPrenom}" style="width: auto;"></strong><br>
        Date de naissance : <strong><input type="text" value="${ageInfo}" style="width: auto;"></strong><br>
        </p>
        <p>
         <br>
         Un total de trois (3) doses :<br><br>
         Jour 0 : <input type="date" id="dateJour0" value="${dateFormattedJour0}" readonly> (dans le deltoïde)<br>
         Jour 7 : <input type="date" id="datePlus7" value="${dateFormattedPlus7}" readonly> <br>
		 Après 12 mois : <input type="date" id="datePlus12Mois" value="${dateFormattedPlus12Mois}" readonly> <br>
        NB: 01 dose=0,1mI
        </p>
        <p style="text-align: right; margin-top: 30px;">
        Medecin traitant <br>
        <span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>&nbsp&nbsp&nbsp&nbsp&nbsp;
        </p>
    </div>

    <div class="print-button">
        <button id="printButton">Imprimer le schéma</button>
		<button id="saveButtonProph">Sauvegarder</button>
    </div>
    

    <script>
    document.addEventListener('DOMContentLoaded', function () {
        const polycliniqueInput = document.getElementById('polyclinique');
        if (polycliniqueInput) {
            polycliniqueInput.addEventListener('input', function () {
                localStorage.setItem('polyclinique', this.value);
            });
        }

        const polycliniqueArInput = document.getElementById('polyclinique-ar');
        if (polycliniqueArInput) {
            polycliniqueArInput.addEventListener('input', function () {
                localStorage.setItem('polyclinique-ar', this.value);
            });
        }

        document.getElementById('printButton').addEventListener('click', function () {
            window.print();
        });
    });
    </script>
    </body>
    </html>
    `;

    var newWindow = window.open("", "_blank");
    if (newWindow) {
        newWindow.document.write(certificatContent);
        newWindow.document.close();
    } else {
        console.log("Popup bloquée par le navigateur.");
    }
}

function demanderDateImmunoDeprime() {
    const today = new Date();
    const year = today.getFullYear();
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const day = String(today.getDate()).padStart(2, '0');
    const todayFormatted = `${year}-${month}-${day}`;

    const modalContent = `
        <div style="background: white; padding: 20px; border-radius: 5px; text-align: center;">
            <h3>Prophylaxie Pré-exposition - Patient Immunodéprimé</h3>
            <p>Date de début de la prophylaxie :</p>
            <input type="date" id="dateDebut" value="${todayFormatted}" style="padding: 8px; margin: 10px 0;">
            <div>
                <button id="confirmDate" style="padding: 8px 16px; background-color: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">Confirmer</button>
            </div>
        </div>`;

    const modal = document.createElement('div');
    modal.style.position = 'fixed';
    modal.style.top = '0';
    modal.style.left = '0';
    modal.style.width = '100%';
    modal.style.height = '100%';
    modal.style.backgroundColor = 'rgba(0, 0, 0, 0.7)';
    modal.style.display = 'flex';
    modal.style.justifyContent = 'center';
    modal.style.alignItems = 'center';
    modal.style.zIndex = '1000';
    modal.innerHTML = modalContent;

    document.body.appendChild(modal);

    modal.querySelector('#confirmDate').addEventListener('click', function () {
        const dateDebut = modal.querySelector('#dateDebut').value;

        if (!dateDebut) {
            alert("Veuillez sélectionner une date.");
            return;
        }

        document.body.removeChild(modal);
        // Appeler la fonction pour générer le certificat de prophylaxie pré-exposition pour immunodéprimé
        genererCertificatProphylaxieImmunoDeprime(dateDebut);
    });

    modal.addEventListener('click', function (e) {
        if (e.target === modal) {
            document.body.removeChild(modal);
        }
    });
}

function prophylaxiePreExpositionSchema3(dateMorsure) {
    // Récupérer les informations du patient depuis le formulaire
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '';
    const patientAge = document.getElementById('patientAge').value || '';
    const patientDateNaissance = document.getElementById('patientDateNaissance').value || '';
    
    // Utiliser la date de naissance si disponible, sinon l'âge
    let ageInfo = '';
    if (patientDateNaissance) {
        ageInfo = patientDateNaissance;
    } else if (patientAge) {
        ageInfo = patientAge;
    } else {
        ageInfo = '[Date de naissance]';
    }
    
    // Calculer les dates pour le schéma Prophylaxie Pré-exposition Schema 3
    const dateJour0 = new Date(dateMorsure);
    const datePlus7 = new Date(dateJour0);
    datePlus7.setDate(dateJour0.getDate() + 7);
    const datePlus21 = new Date(dateJour0);
    datePlus21.setDate(dateJour0.getDate() + 21);
    const datePlus90 = new Date(dateJour0);
    datePlus90.setDate(dateJour0.getDate() + 90);

    const dateFormattedJour0 = `${dateJour0.getFullYear()}-${String(dateJour0.getMonth() + 1).padStart(2, '0')}-${String(dateJour0.getDate()).padStart(2, '0')}`;
    const dateFormattedPlus7 = `${datePlus7.getFullYear()}-${String(datePlus7.getMonth() + 1).padStart(2, '0')}-${String(datePlus7.getDate()).padStart(2, '0')}`;
    const dateFormattedPlus21 = `${datePlus21.getFullYear()}-${String(datePlus21.getMonth() + 1).padStart(2, '0')}-${String(datePlus21.getDate()).padStart(2, '0')}`;
    const dateFormattedPlus90 = `${datePlus90.getFullYear()}-${String(datePlus90.getMonth() + 1).padStart(2, '0')}-${String(datePlus90.getDate()).padStart(2, '0')}`;

    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";
    const docteur = localStorage.getItem('docteur') || "";

    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';

    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"></div>';
    }

    // Générer le certificat Prophylaxie Pré-exposition Schema 3
    const certificatContent = `
    <!DOCTYPE html>
    <html lang="fr">
    <head>
    <meta charset="UTF-8">
    <title>Prophylaxie pré-exposition (avec risque hemoragique </title>
    <style>
    body {
        font-family: Arial, sans-serif;
        padding: 20px;
        background-color: #f9f9f9;
    }
    .certificat {
        background-color: white;
        border: 1px solid #ddd;
        padding: 20px;
        max-width: 600px;
        margin: 0 auto;
        margin-top: 60px;
        box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
    }
    h1 {
        text-align: center;
        color: #333;
        text-decoration: underline;
        font-size: 16px;
    }
    h2 {
        text-align: center;
        color: #555;
        font-size: 10px;
        margin-top: 5px;
        margin-bottom: 15px;
    }
    p {
        line-height: 1.5;
        color: #555;
    }
    .print-button {
        text-align: center;
        margin-top: 20px;
    }
    .print-button button {
        padding: 10px 20px;
        font-size: 16px;
        background-color: #007bff;
        color: white;
        border: none;
        border-radius: 5px;
        cursor: pointer;
    }
    .print-button button:hover {
        background-color: #0056b3;
    }
    @media print {
        @page {
            size: A5;
            margin: 0.2cm 0.2cm 0.2cm 0.2cm;
        }
        body {
            margin: 0 !important;
            padding: 0 !important;
            font-size: 10pt !important;
            background-color: white;
        }
        .certificat {
            padding: 2px 8px !important;
            max-width: 100% !important;
            border: none;
            box-shadow: none;
            margin-top: 0;
        }
        h1 {
            font-size: 14pt !important;
            margin: 5px 0 !important;
            margin-top: 2cm !important;
        }
        h2 {
            font-size: 12pt !important;
            margin: 3px 0 !important;
        }
        p {
            font-size: 9pt !important;
            margin: 2px 0 !important;
            line-height: 1.2 !important;
        }
        input[type="text"],
        input[type="date"],
        textarea {
            border: none !important;
            background: none !important;
            box-shadow: none !important;
            outline: none !important;
            font-size: 9pt !important;
        }
        input[type="text"]:focus,
        input[type="date"]:focus,
        textarea:focus {
            border: none !important;
            outline: none !important;
        }
        .print-button {
            display: none;
        }
        .docteur {
            font-weight: bold;
            font-size: 14pt !important;
            margin-right: 50px;
        }
        /* Additional space optimization */
        * {
            margin-top: 0 !important;
            margin-bottom: 2px !important;
        }
    }
    </style>
    </head>
    <body>
   ${enteteContent}

    <div class="certificat">
        <h1>Prophylaxie Pré-exposition (Avec Risque Hémorragique)</h1>
        <h2>Selon l'instruction N16 du 15 Juillet 2024 relative à la conduite à tenir devant un risque rabique</h2>
        <p>
        NOM : <strong><input type="text" value="${patientNomPrenom}" style="width: auto;"></strong><br>
        Date de naissance : <strong><input type="text" value="${ageInfo}" style="width: auto;"></strong><br>
        </p>
        <p>
         <br>
         Un total de quatre (4) doses :<br><br>
         Jour 0 : <input type="date" id="dateJour0" value="${dateFormattedJour0}" readonly>  (deux [2] doses en ID en deux (2) sites différents)<br>
         Jour 7 : <input type="date" id="datePlus7" value="${dateFormattedPlus7}" readonly>  (deux [2] doses en ID en deux (2) sites différents)<br>
        
         <br><br>
        NB: 01 dose=0,1mI
        </p>
        <p style="text-align: right; margin-top: 30px;">
        Medecin traitant <br>
        <span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>&nbsp&nbsp&nbsp&nbsp&nbsp;
        </p>
    </div>

    <div class="print-button">
        <button id="printButton">Imprimer le schéma</button>
		<button id="saveButtonProphH">Sauvegarder</button>
    </div>
    

    <script>
    document.addEventListener('DOMContentLoaded', function () {
        const polycliniqueInput = document.getElementById('polyclinique');
        if (polycliniqueInput) {
            polycliniqueInput.addEventListener('input', function () {
                localStorage.setItem('polyclinique', this.value);
            });
        }

        const polycliniqueArInput = document.getElementById('polyclinique-ar');
        if (polycliniqueArInput) {
            polycliniqueArInput.addEventListener('input', function () {
                localStorage.setItem('polyclinique-ar', this.value);
            });
        }

        document.getElementById('printButton').addEventListener('click', function () {
            window.print();
        });
    });
    </script>
    </body>
    </html>
    `;

    var newWindow = window.open("", "_blank");
    if (newWindow) {
        newWindow.document.write(certificatContent);
        newWindow.document.close();
    } else {
        console.log("Popup bloquée par le navigateur.");
    }
}

// Fonction pour générer le certificat de prophylaxie pré-exposition pour patients immunodéprimés
function genererCertificatProphylaxieImmunoDeprime(dateDebut) {
    // Get patient information from the form fields
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '';
    const patientAge = document.getElementById('patientAge').value || '';
    const patientDateNaissance = document.getElementById('patientDateNaissance').value || '';
    
    // Extraire nom et prénom
    const nomPrenomArray = patientNomPrenom.split(' ');
    const nom = nomPrenomArray[nomPrenomArray.length - 1] || '';
    const prenom = nomPrenomArray.slice(0, -1).join(' ') || '';
    
    // Utiliser la date de naissance si disponible, sinon l'âge
    let ageInfo = '';
    if (patientDateNaissance) {
        ageInfo = patientDateNaissance;
    } else if (patientAge) {
        ageInfo = patientAge;
    } else {
        ageInfo = '[Date de naissance]';
    }

    const dateJour0 = new Date(dateDebut);
    const datePlus3 = new Date(dateJour0);
    datePlus3.setDate(dateJour0.getDate() + 3);

    const datePlus28 = new Date(dateJour0);
    datePlus28.setDate(dateJour0.getDate() + 28);

    // Ajouter 12 mois (365 jours) pour le rappel
    const datePlus12Mois = new Date(dateJour0);
    datePlus12Mois.setFullYear(dateJour0.getFullYear() + 1);

    const dateFormattedJour0 = `${dateJour0.getFullYear()}-${String(dateJour0.getMonth() + 1).padStart(2, '0')}-${String(dateJour0.getDate()).padStart(2, '0')}`;
    const dateFormattedPlus3 = `${datePlus3.getFullYear()}-${String(datePlus3.getMonth() + 1).padStart(2, '0')}-${String(datePlus3.getDate()).padStart(2, '0')}`;
    const dateFormattedPlus28 = `${datePlus28.getFullYear()}-${String(datePlus28.getMonth() + 1).padStart(2, '0')}-${String(datePlus28.getDate()).padStart(2, '0')}`;
    const dateFormattedPlus12Mois = `${datePlus12Mois.getFullYear()}-${String(datePlus12Mois.getMonth() + 1).padStart(2, '0')}-${String(datePlus12Mois.getDate()).padStart(2, '0')}`;

    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";
    const docteur = localStorage.getItem('docteur') || "";

    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';
    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        enteteContent = '<div style="height: 155px;"></div>';
    }

    const certificatContent = `
    <!DOCTYPE html>
    <html lang="fr">
    <head>
    <meta charset="UTF-8">
    <title>Prophylaxie pré-exposition - Patient Immunodéprimé</title>
    <style>
    body {
        font-family: Arial, sans-serif;
        padding: 14px;
        background-color: #f9f9f9;
    }
    .certificat {
        background-color: white;
        border: 1px solid #ddd;
        padding: 16px;
        max-width: 600px;
        margin: 0 auto;
        margin-top: 60px;
        box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
    }
    h1 {
        text-align: center;
        color: #333;
        text-decoration: underline;
        font-size: 14px;
    }
    h2 {
        text-align: center;
        color: #555;
        font-size: 10px;
        margin-top: 5px;
        margin-bottom: 15px;
    }
    p {
        line-height: 1.5;
        color: #555;
    }
    .small-text {
        font-size: 12px;
    }
    .print-button {
        text-align: center;
        margin-top: 20px;
    }
    .print-button button {
        padding: 10px 20px;
        font-size: 16px;
        background-color: #007bff;
        color: white;
        border: none;
        border-radius: 5px;
        cursor: pointer;
    }
    .print-button button:hover {
        background-color: #0056b3;
    }
    @media print {
        @page {
            size: A5;
            margin: 0.2cm 0.2cm 0.2cm 0.2cm;
        }
        body {
            margin: 0 !important;
            padding: 0 !important;
            font-size: 10pt !important;
            background-color: white;
        }
        .certificat {
            padding: 2px 8px !important;
            max-width: 100% !important;
            border: none;
            box-shadow: none;
            margin-top: 0;
        }
        h1 {
            font-size: 14pt !important;
            margin: 5px 0 !important;
            margin-top: 2cm !important;
        }
        h2 {
            font-size: 12pt !important;
            margin: 3px 0 !important;
        }
        p {
            font-size: 9pt !important;
            margin: 2px 0 !important;
            line-height: 1.2 !important;
        }
        input[type="text"],
        input[type="date"],
        textarea {
            border: none !important;
            background: none !important;
            box-shadow: none !important;
            outline: none !important;
            font-size: 9pt !important;
        }
        input[type="text"]:focus,
        input[type="date"]:focus,
        textarea:focus {
            border: none !important;
            outline: none !important;
        }
        .print-button {
            display: none;
        }
        .docteur {
            font-weight: bold;
            font-size: 14pt !important;
            margin-right: 50px;
        }
        /* Additional space optimization */
        * {
            margin-top: 0 !important;
            margin-bottom: 2px !important;
        }
    }
    </style>
    </head>
    <body>
   ${enteteContent}

    <div class="certificat">
        <h1>Prophylaxie pré-exposition (Immunodéprimé)</h1>
        <h2>Selon l'instruction N16 du 15 Juillet 2024 relative à la conduite à tenir devant un risque rabique</h2>
        <p>
        NOM : <strong><input type="text" value="${nom} ${prenom}" style="width: auto;"></strong><br>
        Date de naissance : <strong><input type="text" value="${ageInfo}" style="width: auto;"></strong><br>
        </p>
        <p>
         <strong style="font-size: 14px;">Patient Immunodéprimé - Prophylaxie pré-exposition :</strong><br>
         Un total de quatre (4) doses :<br><br>
         Jour 0 : <input type="date" id="dateJour0" value="${dateFormattedJour0}" readonly> (dans le deltoïde)<br>
         Jour 3 : <input type="date" id="datePlus3" value="${dateFormattedPlus3}" readonly> <br>
         Jour 28 : <input type="date" id="datePlus28" value="${dateFormattedPlus28}" readonly> <br>
         Après 12 mois : <input type="date" id="datePlus12Mois" value="${dateFormattedPlus12Mois}" readonly> <br>
         <br><br>
         
        </p>
        <p style="text-align: right; margin-top: 30px;">
        Medecin traitant <br>
        <span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>&nbsp&nbsp&nbsp&nbsp&nbsp;
        </p>
    </div>

    <div class="print-button">
        <button id="printButton">Imprimer le schéma</button>
		<button id="saveButtonProphd">Sauvegarder</button>
    </div>
 

    <script>
    document.addEventListener('DOMContentLoaded', function () {
        const polycliniqueInput = document.getElementById('polyclinique');
        if (polycliniqueInput) {
            polycliniqueInput.addEventListener('input', function () {
                localStorage.setItem('polyclinique', this.value);
            });
        }

        const polycliniqueArInput = document.getElementById('polyclinique-ar');
        if (polycliniqueArInput) {
            polycliniqueArInput.addEventListener('input', function () {
                localStorage.setItem('polyclinique-ar', this.value);
            });
        }

        document.getElementById('printButton').addEventListener('click', function () {
            window.print();
        });
    });
    </script>
    </body>
    </html>
    `;

    var newWindow = window.open("", "_blank");
    if (newWindow) {
        newWindow.document.write(certificatContent);
        newWindow.document.close();
    } else {
        console.log("Popup bloquée par le navigateur.");
    }
}

function Tissulairesanssar(dateMorsure, poidsInput) {
    // Récupérer les informations du patient depuis le formulaire
    const patientNomPrenom = document.getElementById('patientNomPrenom').value || '';
    const patientAge = document.getElementById('patientAge').value || '';
    const patientDateNaissance = document.getElementById('patientDateNaissance').value || '';
    
    // Utiliser la date de naissance si disponible, sinon l'âge
    let ageInfo = '';
    if (patientDateNaissance) {
        ageInfo = patientDateNaissance;
    } else if (patientAge) {
        ageInfo = patientAge;
    } else {
        ageInfo = '[Date de naissance]';
    }
    
    // Calculer les dates pour le schéma Tissulaire sans SAR
    const dateJour0 = new Date(dateMorsure);
    const dateJour1 = new Date(dateJour0);
    dateJour1.setDate(dateJour0.getDate() + 1);
    const dateJour2 = new Date(dateJour0);
    dateJour2.setDate(dateJour0.getDate() + 2);
    const dateJour3 = new Date(dateJour0);
    dateJour3.setDate(dateJour0.getDate() + 3);
    const dateJour4 = new Date(dateJour0);
    dateJour4.setDate(dateJour0.getDate() + 4);
    const dateJour5 = new Date(dateJour0);
    dateJour5.setDate(dateJour0.getDate() + 5);
    const dateJour6 = new Date(dateJour0);
    dateJour6.setDate(dateJour0.getDate() + 6);
    const dateJour10 = new Date(dateJour0);
    dateJour10.setDate(dateJour0.getDate() + 10);
    const dateJour14 = new Date(dateJour0);
    dateJour14.setDate(dateJour0.getDate() + 14);
    const dateJour29 = new Date(dateJour0);
    dateJour29.setDate(dateJour0.getDate() + 29);
    const dateJour90 = new Date(dateJour0);
    dateJour90.setDate(dateJour0.getDate() + 90);

    // Fonction pour formater la date
    const formatDate = (date) => `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;

    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";
    const docteur = localStorage.getItem('docteur') || "";
    const animal = "chien"; // Animal en cause (valeur par défaut)

    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';

    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"></div>';
    }

    // Générer le certificat Tissulaire sans SAR
    const certificatContent = `
    <!DOCTYPE html>
    <html lang="fr">
    <head>
    <meta charset="UTF-8">
    <title>Certificat Antirabique - Tissulaire sans SAR</title>
    <style>
    body {
        font-family: Arial, sans-serif;
        padding: 20px;
        background-color: #f9f9f9;
    }
    .certificat {
        background-color: white;
        border: 1px solid #ddd;
        padding: 20px;
        max-width: 600px;
        margin: 0 auto;
        margin-top: 60px;
        box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
    }
    h1 {
        text-align: center;
        color: #333;
        text-decoration: underline;
        font-size: 15px;
        margin-top: 5px;
    }
    p {
        line-height: 1.4;
        color: #555;
    }
    .print-button {
        text-align: center;
        margin-top: 20px;
    }
    .print-button button {
        padding: 10px 20px;
        font-size: 16px;
        background-color: #007bff;
        color: white;
        border: none;
        border-radius: 5px;
        cursor: pointer;
    }
    .print-button button:hover {
        background-color: #0056b3;
    }
    @media print {
        @page {
            size: A5;
            margin: 0.2cm 0.2cm 0.2cm 0.2cm;
        }
        body {
            margin: 0 !important;
            padding: 0 !important;
            font-size: 10pt !important;
            background-color: white;
        }
        .certificat {
            padding: 2px 8px !important;
            max-width: 100% !important;
            border: none;
            box-shadow: none;
            margin-top: 0;
        }
        h1 {
            font-size: 14pt !important;
            margin: 5px 0 !important;
            margin-top: 2cm !important;
        }
        p {
            font-size: 9pt !important;
            margin: 2px 0 !important;
            line-height: 1.2 !important;
        }
        input[type="text"],
        input[type="date"],
        textarea {
            border: none !important;
            background: none !important;
            box-shadow: none !important;
            outline: none !important;
            font-size: 9pt !important;
        }
        input[type="text"]:focus,
        input[type="date"]:focus,
        textarea:focus {
            border: none !important;
            outline: none !important;
        }
        .print-button {
            display: none;
        }
        .docteur {
            font-weight: bold;
            font-size: 14pt !important;
            margin-right: 50px;
        }
        /* Additional space optimization */
        * {
            margin-top: 0 !important;
            margin-bottom: 2px !important;
        }
    }
    </style>
    </head>
    <body>
   ${enteteContent}

    <div class="certificat">
        <h1>Schéma Antirabique - Tissulaire sans SAR</h1>
        <p>
        Nom et Prenom : <strong><input type="text" value="${patientNomPrenom}" style="width: auto;"></strong><br>
        Date de naissance : <strong><input type="text" value="${ageInfo}" style="width: auto;"></strong><br>
        Animal en cause : <strong><input type="text" value="${animal}" style="width: auto;"></strong><br>
        Classe 02, schéma choisi : vaccin tissulaire / sans SAR<br>

        Jour 0 : <input type="date" id="dateJour0" value="${formatDate(dateJour0)}" readonly>( dans les 07 premiers jours les injections sous-cutanée péri ombilicale)<br>
        Jour 1 : <input type="date" id="dateJour1" value="${formatDate(dateJour1)}" readonly><br>
        Jour 2 : <input type="date" id="dateJour2" value="${formatDate(dateJour2)}" readonly><br>
        Jour 3 : <input type="date" id="dateJour3" value="${formatDate(dateJour3)}" readonly><br>
        Jour 4 : <input type="date" id="dateJour4" value="${formatDate(dateJour4)}" readonly><br>
        Jour 5 : <input type="date" id="dateJour5" value="${formatDate(dateJour5)}" readonly><br>
        Jour 6 : <input type="date" id="dateJour6" value="${formatDate(dateJour6)}" readonly><br>
        =================== les rappels en ID dans les deux bras ==================<br>
        Jour 10 : <input type="date" id="dateJour10" value="${formatDate(dateJour10)}" readonly><br>
        Jour 14 : <input type="date" id="dateJour14" value="${formatDate(dateJour14)}" readonly><br>
        Jour 29 : <input type="date" id="dateJour29" value="${formatDate(dateJour29)}" readonly><br>
        Jour 90 : <input type="date" id="dateJour90" value="${formatDate(dateJour90)}" readonly><br>
        <br>
        en cas d'âge <5ans la dose sera 1/2 amp (01 ml)<br>
        </p>
        <p style="text-align: right; margin-top: 15px;">
        Medecin traitant <br>
        <span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>&nbsp&nbsp&nbsp&nbsp&nbsp;
        </p>
    </div>

    <div class="print-button">
        <button id="printButton">Imprimer le schéma</button>
		<button id="saveButtonT2">Sauvegarder</button>
    </div>
   

    <script>
    document.addEventListener('DOMContentLoaded', function () {
        const polycliniqueInput = document.getElementById('polyclinique');
        if (polycliniqueInput) {
            polycliniqueInput.addEventListener('input', function () {
                localStorage.setItem('polyclinique', this.value);
            });
        }

        const polycliniqueArInput = document.getElementById('polyclinique-ar');
        if (polycliniqueArInput) {
            polycliniqueArInput.addEventListener('input', function () {
                localStorage.setItem('polyclinique-ar', this.value);
            });
        }

        document.getElementById('printButton').addEventListener('click', function () {
            window.print();
        });
    });
    </script>
    </body>
    </html>
    `;

    var newWindow = window.open("", "_blank");
    if (newWindow) {
        newWindow.document.write(certificatContent);
        newWindow.document.close();
    } else {
        console.log("Popup bloquée par le navigateur.");
    }
}