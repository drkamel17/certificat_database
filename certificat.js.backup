// Style pour forcer la taille de police des champs date à  11px
const style = document.createElement('style');
style.textContent = `
    input[type="date"] {
        font-size: 11px !important;
    }
`;
document.head.appendChild(style);

// Fonction personnalisée pour conserver les retours à  la ligne
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

document.addEventListener('DOMContentLoaded', () => {
    loadData(); // Chargement des données de la polyclinique et du docteur
    loadPatientData(); // Chargement des données du patient
    loadTargetUrl(); // Chargement de l'URL cible
    storeActiveTabId(); // Stockage de l'ID de l'onglet actif

    // Initialiser le format au chargement
    const format = localStorage.getItem('certificatFormat');
    if (format === 'sansEntete') {
        document.getElementById('formatSansEntete').classList.add('selected-format');
    } else {
        // Par défaut, on utilise avec en-tete
        document.getElementById('formatAvecEntete').classList.add('selected-format');
    }


    // Ecouteurs pour les boutons
    setupButtonListeners();
    document.getElementById("brucellose").addEventListener("click", () => {

        // Ouvre ord.html dans un nouvel onglet
        chrome.tabs.create({ url: chrome.runtime.getURL('brucellose.html') });

    });

    document.getElementById("permisVierge").addEventListener("click", () => {
        // Ouvre le fichier PDF du permis vierge
        chrome.tabs.create({ url: chrome.runtime.getURL('templates/permis.pdf') });
    });

    // Ecouteur pour le bouton Certificat Prénuptial
    document.getElementById("prenuptial").addEventListener("click", () => {
        // Ouvre prenuptial.html dans un nouvel onglet
        chrome.tabs.create({ url: chrome.runtime.getURL('prenuptial.html') });
    });

    // Configuration des écouteurs spécifiques
    document.getElementById("inaptSport").addEventListener("click", () => {
        ouvrirCertificatInaptitudeSport();
    });


    // Ecouteur pour le bouton Catégorie Anti-Rabique
    document.getElementById('catAntiRabique').addEventListener('click', function () {
        const classe02 = document.getElementById('classe02');
        const classe03 = document.getElementById('classe03');
        const prex = document.getElementById('prex');
        classe02.classList.toggle('hidden');
        classe03.classList.toggle('hidden');
        prex.classList.toggle('hidden');
    });

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
                // Rafraà®chir la page
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
<button id="prophylaxiePreExpositionSchema3" style="padding: 8px 16px; margin: 5px; background-color: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">03 Avec Risque Hemoragique</button>
</div>
`;
        openModal(modalContent);
        // Écouteur pour le bouton Immunocompétent
        document.querySelector('#immunocompetent').addEventListener('click', () => {
            // Demander la date de début et le poids
            demanderDateEtPoidsImmunocompetent();
        });

        // Écouteur pour le bouton Immunodéprimé
        document.querySelector('#immunodeprime').addEventListener('click', () => {
            // Demander la date de début pour patient immunodéprimé
            demanderDateImmunoDeprime();
        });

        // Écouteur pour le bouton Avec ATCD Prophylaxie Pré-exposition
        document.querySelector('#prophylaxiePreExpositionSchema3').addEventListener('click', () => {
            // Demander la date et le poids pour ATCD Prophylaxie Pré-exposition
            demanderDateEtPoidsATCDProphylaxie();
        });
    }

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






// Fonction pour configurer les écouteurs de boutons
function setupButtonListeners() {
    const buttonIds = [
        'genererCertificat',
        'justification',
        'genererArret',
        'genererProlongation',
        'genererRadiox',
        'genererLettre',
        'genererChronique',
        'genererReprise',
        'cbv',
        'genererLeishmaniose',
        'requisition',
        'SavePolycliniqueDocteur',
        'NON-GROSSESSE',
        'bonSante',
        'malVision'
    ];



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

    buttonIds.forEach(buttonId => {
        const buttonElement = document.getElementById(buttonId);
        if (buttonElement) {
            buttonElement.addEventListener('click', handleButtonClick);
        } else {
            console.warn(`Element with ID "${buttonId}" not found. Cannot attach event listener.`);
        }
    });
    // Boutons de format
    // Boutons de format
    document.getElementById('formatAvecEntete').addEventListener('click', () => {
        localStorage.setItem('certificatFormat', 'avecEntete');
        document.getElementById('formatAvecEntete').classList.add('selected-format');
        document.getElementById('formatSansEntete').classList.remove('selected-format');
        showFormatStatus('Format avec en-tete sélectionné');
    });

    document.getElementById('formatSansEntete').addEventListener('click', () => {
        localStorage.setItem('certificatFormat', 'sansEntete');
        document.getElementById('formatSansEntete').classList.add('selected-format');
        document.getElementById('formatAvecEntete').classList.remove('selected-format');
        showFormatStatus('Format sans en-tete sélectionné');
    });

}






function showFormatStatus(message) {
    const status = document.createElement('div');
    status.textContent = message;
    status.style.position = 'fixed';
    status.style.bottom = '10px';
    status.style.left = '10px';
    status.style.backgroundColor = '#4CAF50';
    status.style.color = 'white';
    status.style.padding = '10px';
    status.style.borderRadius = '5px';
    status.style.zIndex = '1000';
    document.body.appendChild(status);

    setTimeout(() => {
        status.remove();
    }, 2000);
}


// Fonction pour gérer les clics sur les boutons
function handleButtonClick(event) {
    const id = event.target.id;
    switch (id) {
        case 'genererCertificat':
            ouvrirCertificat();
            break;
        case 'justification':
            justification();
            break;
        case 'genererArret':
            ouvrirCertificatArret();
            break;
        case 'genererProlongation':
            ouvrirCertificatProlongation();
            break;
        case 'genererRadiox':
            ouvrirCertificatRadiox();
            break;
        case 'genererLettre':
            ouvrirCertificatLettre();
            break;
        case 'genererChronique':
            ouvrirCertificatChronique();
            break;
        case 'genererReprise':
            ouvrirCertificatReprise();
            break;
        case 'cbv':
            cbv();
            break;
        case 'genererLeishmaniose':
            ouvrirCertificatLeishmaniose();
            break;
        case 'requisition':
            requisition();
            break;
        case 'NON-GROSSESSE':
            ouvrirCertificatNonGrossesse();
            break;
        case 'inaptSport':
            ouvrirCertificatInaptitudeSport();
            break;
        case 'bonSante':
            ouvrirCertificatBonSante();
            break;
        case 'malVision':
            ouvrirCertificatMalVision();
            break;
        case 'SavePolycliniqueDocteur':
            savePolycliniqueDocteur();
            break;
        default:
            console.warn('Bouton non géré:', id);
    }
}

function prophylaxiePreExpositionSchema3(dateMorsure) {
    const { nom, prenom, dob } = patientInfo;

    const dateJour0 = new Date(dateMorsure);
    const datePlus7 = new Date(dateJour0);
    datePlus7.setDate(dateJour0.getDate() + 7);

    const dateFormattedJour0 = `${dateJour0.getFullYear()}-${String(dateJour0.getMonth() + 1).padStart(2, '0')}-${String(dateJour0.getDate()).padStart(2, '0')}`;
    const dateFormattedPlus7 = `${datePlus7.getFullYear()}-${String(datePlus7.getMonth() + 1).padStart(2, '0')}-${String(datePlus7.getDate()).padStart(2, '0')}`;

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
    <title>Certificat Antirabique - Avec ATCD Prophylaxie Pré-exposition </title>
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
        <h1>Prophylaxie pré-exposition (avec risque hemoragique ) </h1>
        <h2>Selon l'instruction N16 du 15 Juillet 2024 relative à  la conduite à  tenir devant un risque rabique</h2>
        <p>
        NOM : <strong><input type="text" value="${nom} ${prenom}" style="width: auto;"></strong><br>
        Date de naissance : <strong><input type="text" value="${dob}" style="width: auto;"></strong><br>
        </p>
        <p>
         <br>
         Un total de quatre (4) doses :<br><br>
         Jour 0 : <input type="date" id="dateJour0" value="${dateFormattedJour0}" readonly> (deux [2] doses en ID en deux (2) sites différents)<br>
         Jour 7 : <input type="date" id="datePlus7" value="${dateFormattedPlus7}" readonly> (deux [2] doses en ID en deux (2) sites différents)<br>
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
    </div>
    <script src="print.js"></script>

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



function prophylaxiePreExpositionSchema1(dateMorsure, poidsInput) {
    const { nom, prenom, dob } = patientInfo;
    const poids = parseFloat(poidsInput);
    const animal = "chien"; // Animal en cause (valeur par défaut)

    const dateJour0 = new Date(dateMorsure);
    const datePlus3 = new Date(dateJour0);
    datePlus3.setDate(dateJour0.getDate() + 3);

    const dateFormattedJour0 = `${dateJour0.getFullYear()}-${String(dateJour0.getMonth() + 1).padStart(2, '0')}-${String(dateJour0.getDate()).padStart(2, '0')}`;
    const dateFormattedPlus3 = `${datePlus3.getFullYear()}-${String(datePlus3.getMonth() + 1).padStart(2, '0')}-${String(datePlus3.getDate()).padStart(2, '0')}`;

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
    <title>Certificat Antirabique - Avec ATCD Vaccinaux (Schéma 1)</title>
    <style>
    body {
        font-family: Arial, sans-serif;
        padding:14x;
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
        <h1>Schéma Antirabique -  Avec ATCD Vaccinaux(Schéma 1)</h1>
        <h2>Selon l'instruction N16 du 15 Juillet 2024 relative à  la conduite à  tenir devant un risque rabique</h2>
        <p>
        NOM : <strong><input type="text" value="${nom} ${prenom}" style="width: auto;"></strong><br>
        Date de naissance : <strong><input type="text" value="${dob}" style="width: auto;"></strong><br>
		Animal en cause : <strong><input type="text" value="${animal}" style="width: auto;"></strong><br>
        Poids : <strong><input type="text" value="${poidsInput} kg" style="width: auto;"></strong><br>
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
		<button id="saveButtonRc2">sauvegarder</button>
    </div>
    <script src="print.js"></script>

    <script>
    document.addEventListener('DOMContentLoaded', function () {
        document.getElementById('polyclinique').addEventListener('input', function () {
            localStorage.setItem('polyclinique', this.value);
        });

        document.getElementById('polyclinique-ar').addEventListener('input', function () {
            localStorage.setItem('polyclinique-ar', this.value);
        });

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



// Fonction pour sauvegarder les informations de la polyclinique et du docteur
// Fonction pour générer le certificat de non-grossesse
function ouvrirCertificatNonGrossesse() {
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

function savePolycliniqueDocteur() {
    const polyclinique = trimPreserveNewlines(document.getElementById('polyclinique').value);
    const polycliniqueAr = trimPreserveNewlines(document.getElementById('polyclinique-ar').value);
    const docteur = trimPreserveNewlines(document.getElementById('docteur').value);

    if (polyclinique && polycliniqueAr && docteur) {
        localStorage.setItem('polyclinique', polyclinique);
        localStorage.setItem('polyclinique-ar', polycliniqueAr);
        localStorage.setItem('docteur', docteur);
        alert("Les informations ont été sauvegardées avec succès !");
    } else {
        alert("Veuillez remplir tous les champs.");
    }
}


function prophylaxiePreExpositionSchema2(dateMorsure, poidsInput) {
    const { nom, prenom, dob } = patientInfo;
    const poids = parseFloat(poidsInput);
    const animal = "chien"; // Animal en cause (valeur par défaut)

    const dateJour0 = new Date(dateMorsure);

    const dateFormattedJour0 = `${dateJour0.getFullYear()}-${String(dateJour0.getMonth() + 1).padStart(2, '0')}-${String(dateJour0.getDate()).padStart(2, '0')}`;

    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";
    const docteur = localStorage.getItem('docteur') || "";

    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';
    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        enteteContent = '<div style="height: 155px;">\</div>';
    }

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
        <h2>Selon l'instruction N16 du 15 Juillet 2024 relative à  la conduite à  tenir devant un risque rabique</h2>
        <p>
        NOM : <strong><input type="text" value="${nom} ${prenom}" style="width: auto;"></strong><br>
        Date de naissance : <strong><input type="text" value="${dob}" style="width: auto;"></strong><br>
		Animal en cause : <strong><input type="text" value="${animal}" style="width: auto;"></strong><br>
        Poids : <strong><input type="text" value="${poidsInput} kg" style="width: auto;"></strong><br>
        </p>
        <p>
         <strong>Personne ayant reçu une prophylaxie Pré-exPosition, ou (≥ 02 doses):</strong><br>
         Schéma 2 : Un total de quatre (4) doses<br><br>
         Jour 0 : <input type="date" id="dateJour0" value="${dateFormattedJour0}" readonly> (une dose en ID en quatre (4) sites différents)<br>
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
		<button id="saveButtonRc2">sauvegarder</button>
    </div>
    <script src="print.js"></script>

    <script>
    document.addEventListener('DOMContentLoaded', function () {
        document.getElementById('polyclinique').addEventListener('input', function () {
            localStorage.setItem('polyclinique', this.value);
        });

        document.getElementById('polyclinique-ar').addEventListener('input', function () {
            localStorage.setItem('polyclinique-ar', this.value);
        });

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




// Fonction pour charger l'URL cible et l'afficher
function loadTargetUrl() {
    chrome.tabs.query({ active: true, currentWindow: true }, (tabs) => {
        const targetUrlDisplay = document.getElementById('targetUrlDisplay');
        if (targetUrlDisplay) {
            if (tabs.length > 0) {
                const targetUrl = tabs[0].url;
                localStorage.setItem('targetUrl', targetUrl);
                targetUrlDisplay.textContent = targetUrl;
            } else {
                targetUrlDisplay.textContent = 'Aucune URL cible trouvée.';
            }
        } else {
            console.warn('Element targetUrlDisplay non trouvé dans le DOM');
        }
    });
}

// Fonction pour charger les données du patient
function loadPatientData() {
    chrome.tabs.query({ active: true, currentWindow: true }, (tabs) => {
        chrome.tabs.executeScript(tabs[0].id, { code: `(${extractInfo})()` }, (results) => {
            if (results && results[0]) {
                console.log("Données patient chargées:", results[0]);
            }
        });
    });
}

// Fonction pour charger les données de la polyclinique et du docteur
function loadData() {
    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";
    const docteur = localStorage.getItem('docteur') || "";

    document.getElementById('polyclinique').value = polyclinique;
    document.getElementById('polyclinique-ar').value = polycliniqueAr;
    document.getElementById('docteur').value = docteur;
}

// Fonction pour stocker l'ID de l'onglet actif
function storeActiveTabId() {
    chrome.tabs.query({ active: true, currentWindow: true }, (tabs) => {
        if (tabs[0]) {
            localStorage.setItem('targetTabId', tabs[0].id);
        }
    });
}

// Fonction pour extraire les informations de la page
function extractInfo() {
    // Récupérer les données depuis localStorage avec capitalisation
    const nom = capitalizeNames(localStorage.getItem("nom") || "");
    const prenom = capitalizeNames(localStorage.getItem("prenom") || "");
    const dob = localStorage.getItem("dateNaissance") || "";
    const numero = localStorage.getItem("numero") || ""
    return { nom, prenom, dob, numero };
}


// Fonction pour injecter le contenu
function injectContent() {
    const targetTabId = localStorage.getItem('targetTabId');
    if (!targetTabId) {
        console.error('Erreur: Impossible de récupérer l\'ID de l\'onglet');
        return;
    }

    const injectCode = `
        (function() {
            const editableDiv = document.querySelector('div[contenteditable="true"]');
            if (!editableDiv) throw new Error('Zone éditable introuvable');

            const today = new Date();
            const dateStr = \`\${String(today.getDate()).padStart(2, '0')}/\${String(today.getMonth() + 1).padStart(2, '0')}/\${today.getFullYear()}\`;
            editableDiv.innerHTML += '<br>Eviction scolaire à  ce jour: ' + dateStr;

            return { success: true };
        })();
    `;

    chrome.tabs.executeScript(parseInt(targetTabId), { code: injectCode })
        .then(results => {
            if (results && results[0]?.success) {
                console.log('Injection réussie');
            } else {
                console.error('Erreur lors de l\'injection');
            }
        })
        .catch(error => {
            console.error('Erreur d\'injection:', error);
        });
}


function injectContent2() {
    const originalTabId = localStorage.getItem('originalTabId'); // Récupérer l'ID

    if (!originalTabId) {
        showStatus('Onglet cible introuvable', 'error');
        return;
    }

    const injectCode = `
        (function() {
            const editableDiv = document.querySelector('div[contenteditable="true"]');
            if (!editableDiv) throw new Error('Zone éditable introuvable');

            const today = new Date();
            const dateStr = \`\${String(today.getDate()).padStart(2, '0')}/\${String(today.getMonth() + 1).padStart(2, '0')}/\${today.getFullYear()}\`;
            editableDiv.innerHTML += '<br>Eviction scolaire à  ce jour: ' + dateStr;

            return { success: true };
        })();
    `;

    chrome.tabs.executeScript(parseInt(originalTabId), { code: injectCode })
        .then(results => {
            if (results[0].success) {
                showStatus('Injection réussie', 'success');
            } else {
                showStatus('Erreur: ' + results[0].error, 'error');
            }
        })
        .catch(error => {
            showStatus('Erreur d\'injection: ' + error.message, 'error');
            console.error('Erreur d\'injection:', error);
        });
}
// Note: Removed excessive parentheses that were causing TypeScript parsing errors


// Ecouteur pour le bouton de catégorie de leishmaniose









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
        // Get the date and weight input from storage
        const dateMorsure = localStorage.getItem('dateMorsure') || new Date().toISOString().split('T')[0];
        const poidsInput = localStorage.getItem('poidsInput') || '';
        openVaccinChoiceModal(dateMorsure, poidsInput); // Pass the date and weight
    });

    // Ecouteur pour le bouton Vaccin Tissulaire
    document.querySelector('#vaccinTissulaire').addEventListener('click', () => {
        const dateMorsure = localStorage.getItem('dateMorsure') || new Date().toISOString().split('T')[0];
        const poidsInput = localStorage.getItem('poidsInput') || '';
        Tissulairesanssar(dateMorsure, poidsInput); // Pass the date and weight
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

// Exemple de fonction zegreb
function zegreb() {
    alert("Vaccin Cellulaire de Zagreb sélectionné.");
}

// Exemple de fonction essens02
function essens() {
    alert("Vaccin Cellulaire d'Essen sélectionné.");
}





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

function ouvrirModalChoixSchema(dateMorsure, poidsInput) {
    const modalContent = `
    <div style="background: white; padding: 20px; border-radius: 5px; text-align: center;">
        <h3 style="color: green;">Choisissez un schéma :</h3>
        <button id="zegreb3" style="padding: 8px 16px; margin: 5px; background-color: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">Zagreb3</button>
        <button id="essen3" style="padding: 8px 16px; margin: 5px; background-color: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">Essen3</button>
        <button id="tissulaireAvecSAR" style="padding: 8px 16px; margin: 5px; background-color: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">Tissulaire avec SAR</button>
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

    // Ecouteur pour le bouton Tissulaire avec SAR
    modal.querySelector('#tissulaireAvecSAR').addEventListener('click', () => {
        document.body.removeChild(modal);
        vaccint3(dateMorsure, poidsInput);
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

// Fonction pour demander la date et le poids pour les patients immunocompétents
function demanderDateEtPoidsImmunocompetent() {
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







// Fonctions spécifiques pour chaque type de certificat

// Fonction pour la requisition apte
function requisitionApte1() {
    const { nom, prenom, dob } = patientInfo;

    const today = new Date();
    const year = today.getFullYear();
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const day = String(today.getDate()).padStart(2, '0');
    const todayFormatted = `${year}-${month}-${day}`;

    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";
    const docteur = localStorage.getItem('docteur') || "";

    const avecEntete = localStorage.getItem('certificatFormat') !== 'sansEntete';

    let enteteContent = '';
    if (avecEntete) {
        enteteContent = `${generateHeader()}`;
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 155px;"></div>';
    }

    const requisitionContent = `
    <!DOCTYPE html>
    <html lang="fr">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Réquisition Médicale</title>
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
                box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
                margin-top: 80px;
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
            input[type="text"], textarea {
                border: none;
                background: transparent;
                font-family: inherit;
                font-size: inherit;
                color: inherit;
                outline: none;
            }
            textarea {
                width: 100%;
                height: 100px;
                margin-top: 10px;
                resize: none;
            }
            .decision-container {
                position: relative;
                margin: 20px 0;
            }
            .decision-option {
                margin-bottom: 10px;
            }
            .print-button {
                margin-top: 30px;
                text-align: center;
            }
            .print-button button {
                padding: 10px 20px;
                background-color: #4CAF50;
                color: white;
                border: none;
                border-radius: 4px;
                cursor: pointer;
                font-family: inherit;
                font-size: 14px;
            }
            .print-button button:hover {
                background-color: #45a049;
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
                Je soussigné(e), Dr <input type="text" id="docteur" value="${docteur}" readonly style="width: 120px;">, 
                certifie avoir examiné ce jour <strong><input type="text" value="${nom} ${prenom}" readonly style="width: 180px;"></strong>, 
                né(e) le <strong><input type="text" value="${dob}" readonly style="width: 100px;"></strong>.
            </p>
            <p>
                suite à  la réquisition numéro <input type="text" placeholder="Numéro de réquisition" style="width: 100px;">
            </p>
            <div class="decision-container">
                <div class="decision-option" id="compatible-section">
                    <label for="compatible">Après un examen médical, je déclare que le sus nommé est compatible avec les conditions de garde à  vue.</label>
                    <textarea placeholder="Ajouter des notes complémentaires"></textarea>
                </div>
            </div>
            <p style="text-align: right; margin-top: 30px;">
                Le présent certificat est remis à  l'autorité pour servir et valoir ce que de droit.
            </p>
            <p style="text-align: right; margin-top: 30px;">
                Dont certificat<br>
                <span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>
            </p>
            </div>
        </div>

        <div class="print-button">
            <button id="printButton">Imprimer</button>
        </div>
        <script>
        document.getElementById('printButton').addEventListener('click', function() {
            window.print();
        });
        </script>
    </body>
    </html>
    `;

    // Ouvrir une nouvelle fenetre avec l'ordonnance
    const newWindow = window.open("", "_blank");
    if (newWindow) {
        // Créer un lien d'impression dans la fenetre principale
        const printLink = document.createElement('a');
        printLink.href = '#';
        printLink.textContent = 'Imprimer';
        printLink.onclick = function () {
            newWindow.print();
            return false;
        };
        printLink.className = 'print-button';
        document.body.appendChild(printLink);
    }
    if (newWindow) {
        newWindow.document.write(requisitionContent);
        newWindow.document.close();
        // Forcer l'impression automatiquement
        setTimeout(() => {
            newWindow.print();
        }, 1000);
    } else {
        console.log("Popup bloquée par le navigateur.");
    }
}



// Fonction pour la requisition inapte
function requisitionInapte() {
    // Fermer la modale si elle existe
    const existingModal = document.querySelector('.modal');
    if (existingModal) {
        existingModal.remove();
        window.location.reload();
    }

    const { nom, prenom, dob } = patientInfo;

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
    <h1>CERTIFICAT MEDICAL D'INAPTITUDE AU GARDE-à€-VUE</h1>
    <div class="contenu-certificat" style="margin-top: 1.5cm !important;">
    <p>
      Je soussigné(e), Dr 
      <input type="text" value="${docteur}" readonly style="width: 120px;">, 
      certifie avoir examiné ce jour le nomee 
      <strong><input type="text" value="${nom} ${prenom}" readonly placeholder="Nom et prénom" style="width: 180px; padding: 4px; border: 1px solid #ddd; border-radius: 4px; margin: 0 5px;"></strong>
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




function ouvrirCertificat() {
    const { nom, prenom, dob, url } = extractInfo();
    const today = new Date();
    const year = today.getFullYear();
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const day = String(today.getDate()).padStart(2, '0');
    const todayFormatted = `${year}-${month}-${day}`;

    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";
    const docteur = localStorage.getItem('docteur') || "";

    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') !== 'sansEntete';

    let enteteContent = '';
    if (avecEntete) {
        enteteContent = `
        ${generateHeader()}
        `;
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
<strong><input type="text" value="${nom} ${prenom}" style="width: 150px;"></strong>,
<span class="editable-field" contenteditable="true" style="min-width: 100px; display: inline-block;">né(e) le ${dob}</span>.
</p>
<p>
déclare que son état de santé nécessite une éviction scolaire de
<input type="text" placeholder="1 (un)" style="width: 80px;"> Jour(s)
à daté du <span class="editable-field" contenteditable="true" style="min-width: 120px; display: inline-block;">${todayFormatted}</span> <br>
sauf complication.
</p>
<p>
<textarea placeholder=" " style="width: 450px;"></textarea>
</p>
<p style="text-align: right; margin-top: 30px;">
Dont certificat&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp<br>
<span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp;
<p>
<p>

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
    // Appliquer la taille de police sauvegardée et masquer les éléments non désirés à  l'impression
    document.addEventListener('DOMContentLoaded', () => {
        const savedFontSize = localStorage.getItem('certificatFontSize') || '14';
        const styleElement = document.createElement('style');
        styleElement.textContent = "@media print { .print-button { display: none !important; } }";
        styleElement.id = 'certificatFontSizeStyle';
        document.head.appendChild(styleElement);
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







// Fonction pour capitaliser automatiquement les noms et prénoms
function capitalizeNames(text) {
    if (!text) return text;
    return text.toLowerCase().replace(/\b\w/g, letter => letter.toUpperCase());
}

// Variables globales pour stocker les infos patient
let patientInfo = {
    nom: " ",
    prenom: " ",
    dob: " "
};
// Fonction pour charger les données au démarrage
function loadPatientData() {
    chrome.tabs.query({ active: true, currentWindow: true }, function (tabs) {
        chrome.tabs.executeScript(tabs[0].id, {
            code: `(${extractInfo})()`
        }, (results) => {
            if (results && results[0]) {
                patientInfo = results[0];
                console.log("Données patient chargées:", patientInfo);
            }
        });
    });
}

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
                        <div style="width: 100%; font-size: 12px; white-space: pre-wrap;">${polyclinique}</div>
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

function ouvrirCertificatArret() {

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
        <h1>Certificat d'arret de Travail</h1>
        <div class="contenu-certificat" style="margin-top: 1.5cm !important;">
        <p>
            Je soussigné, Dr <input type="text" id="docteur" value="${docteur}" placeholder="" style="width: 120px;">, 
            certifie avoir examiné ce jour le(la) susnommé(e) 
            <strong><input type="text" value="${nom} ${prenom}" style="width: 180px;"></strong>,
            <span class="editable-field" contenteditable="true" style="min-width: 100px; display: inline-block;">né(e) le ${dob}</span>.
        </p>
        <p>
            Déclare que son état de santé nécessite un arret de travail de
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
    </div>
    <script src="print.js"></script>
    <script src="certificat-unified-font-size.js"></script>

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
            // Prendre le texte complet sans validation de format
            const editableFields = newWindow.document.querySelectorAll('.editable-field');
            let dateNaissance = '';
            for (let field of editableFields) {
                const text = field.textContent || field.innerText || '';
                // Chercher le champ qui contient "né(e) le" ou similaire
                const parentText = field.parentElement ? field.parentElement.textContent || '' : '';
                if (parentText.includes('né(e)') || parentText.includes('né') || parentText.includes('née')) {
                    // Prendre tout le texte du champ sans validation
                    dateNaissance = text.trim();
                    break;
                }
            }

            // Si pas trouvé, prendre le premier champ editable-field
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
                    if (response && response.ok) {
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
                // Si la fonction n'est pas encore disponible, attendre un peu
                setTimeout(() => {
                    const fn = window.sauvegarderArretTravailDepuisPopup;
                    if (fn && typeof fn === 'function') {
                        fn(message).then(response => {
                            if (response && response.ok) {
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
                // Supprimer les anciens écouteurs en clonant le bouton
                const newPrintButton = printButton.cloneNode(true);
                printButton.parentNode.replaceChild(newPrintButton, printButton);

                newPrintButton.addEventListener('click', function () {
                    // Récupérer le nombre de jours
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

                    // Imprimer avec un léger délai pour laisser le temps à la sauvegarde de démarrer
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

                    // Créer le bouton de sauvegarde
                    const boutonSauvegarde = newWindow.document.createElement('button');
                    boutonSauvegarde.id = 'sauvegarderArretPopup';
                    boutonSauvegarde.innerHTML = '💾 Sauvegarder Arrêt';
                    boutonSauvegarde.style.cssText = 'background-color: #28a745; color: white; border: none; padding: 10px 20px; margin-left: 15px; border-radius: 5px; cursor: pointer; font-size: 16px; transition: background-color 0.3s;';

                    // Ajouter l'effet hover
                    boutonSauvegarde.addEventListener('mouseenter', function () {
                        this.style.backgroundColor = '#218838';
                    });
                    boutonSauvegarde.addEventListener('mouseleave', function () {
                        this.style.backgroundColor = '#28a745';
                    });

                    // Ajouter le bouton à côté du bouton d'impression
                    printButton.parentNode.appendChild(boutonSauvegarde);

                    // Ajouter l'événement de sauvegarde
                    boutonSauvegarde.addEventListener('click', function () {
                        console.log('💾 Clic sur le bouton de sauvegarde');

                        // Récupérer le nombre de jours
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
                            // Créer un prompt personnalisé dans la popup
                            const promptDiv = newWindow.document.createElement('div');
                            promptDiv.id = 'promptNombreJours';
                            promptDiv.style.cssText = 'position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%); z-index: 10001; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3); min-width: 300px;';
                            promptDiv.innerHTML = '<div style="margin-bottom: 15px; font-weight: bold; font-size: 16px;">Nombre de jours d\'arrêt de travail</div><input type="number" id="inputNombreJours" value="1" min="1" style="width: 100%; padding: 8px; border: 1px solid #ccc; border-radius: 4px; font-size: 14px; margin-bottom: 15px;"><div style="display: flex; gap: 10px; justify-content: flex-end;"><button id="btnPromptOk" style="padding: 8px 20px; background: #28a745; color: white; border: none; border-radius: 4px; cursor: pointer;">OK</button><button id="btnPromptCancel" style="padding: 8px 20px; background: #6c757d; color: white; border: none; border-radius: 4px; cursor: pointer;">Annuler</button></div>';

                            // Créer un overlay
                            const overlay = newWindow.document.createElement('div');
                            overlay.id = 'promptOverlay';
                            overlay.style.cssText = 'position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0, 0, 0, 0.5); z-index: 10000;';

                            newWindow.document.body.appendChild(overlay);
                            newWindow.document.body.appendChild(promptDiv);

                            // Focus sur l'input
                            setTimeout(() => {
                                const input = newWindow.document.getElementById('inputNombreJours');
                                if (input) {
                                    input.focus();
                                    input.select();
                                }
                            }, 100);

                            // Gérer les boutons
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

                            // Gérer Enter et Escape
                            const input = newWindow.document.getElementById('inputNombreJours');
                            input.addEventListener('keydown', (e) => {
                                if (e.key === 'Enter') {
                                    btnOk.click();
                                } else if (e.key === 'Escape') {
                                    btnCancel.click();
                                }
                            });
                        } else {
                            // Vérifier que le nombre de jours est valide
                            if (!nombreJours || isNaN(nombreJours) || parseInt(nombreJours) <= 0) {
                                afficherMessageDansPopup('Veuillez entrer un nombre de jours valide', 'warning');
                                return;
                            }
                            // Effectuer la sauvegarde directement
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

            // Utiliser la fonction envoyerMessageNatif si disponible
            if (typeof envoyerMessageNatif === 'function') {
                return await envoyerMessageNatif(message);
            } else if (typeof browser !== 'undefined' && browser.runtime && browser.runtime.sendNativeMessage) {
                return await browser.runtime.sendNativeMessage("com.daoudi.certificat", message);
            } else if (typeof chrome !== 'undefined' && chrome.runtime && chrome.runtime.sendNativeMessage) {
                return await new Promise((resolve, reject) => {
                    chrome.runtime.sendNativeMessage("com.daoudi.certificat", message, (response) => {
                        if (chrome.runtime.lastError) {
                            reject(new Error(chrome.runtime.lastError.message));
                        } else {
                            resolve(response);
                        }
                    });
                });
            } else {
                throw new Error('Aucune API de messagerie native disponible');
            }
        };

        // Exposer la fonction dans la fenêtre parent
        window.sauvegarderArretTravailDepuisPopup = sauvegarderFn;

        // Stocker la référence à la fenêtre parent dans la popup pour qu'elle soit accessible
        newWindow._parentWindowRef = window;

        // Créer la fonction wrapper directement dans la popup après que le document soit chargé
        // Cette fonction utilisera postMessage pour communiquer avec la fenêtre parent
        function setupSauvegardeFunction() {
            newWindow.sauvegarderArretTravailDepuisPopup = function (message) {
                return new Promise((resolve, reject) => {
                    // Utiliser la référence stockée à la fenêtre parent
                    const parentWin = newWindow._parentWindowRef;
                    if (!parentWin) {
                        reject(new Error('Fenêtre parent non accessible'));
                        return;
                    }

                    // Générer un ID unique pour cette requête
                    const requestId = Date.now() + Math.random();

                    // Écouter la réponse
                    const messageHandler = function (event) {
                        if (event.data && event.data.action === 'sauvegarde_result' && event.data.requestId === requestId) {
                            newWindow.removeEventListener('message', messageHandler);
                            if (event.data.success) {
                                resolve(event.data.response);
                            } else {
                                reject(new Error(event.data.error || 'Erreur inconnue'));
                            }
                        }
                    };
                    newWindow.addEventListener('message', messageHandler);

                    // Envoyer la requête à la fenêtre parent
                    parentWin.postMessage({
                        action: 'sauvegarder_arret_travail',
                        message: message,
                        requestId: requestId
                    }, '*');

                    // Timeout après 10 secondes
                    setTimeout(() => {
                        newWindow.removeEventListener('message', messageHandler);
                        reject(new Error('Timeout: La sauvegarde prend trop de temps'));
                    }, 10000);
                });
            };
        }

        // Configurer la fonction après que le document soit chargé
        newWindow.addEventListener('load', setupSauvegardeFunction);

        // Aussi essayer immédiatement au cas où le document est déjà chargé
        if (newWindow.document.readyState === 'complete') {
            setupSauvegardeFunction();
        }

        // Écouter les messages de la popup dans la fenêtre parent
        window.addEventListener('message', async function (event) {
            // Vérifier que le message vient de la popup
            if (event.source === newWindow && event.data && event.data.action === 'sauvegarder_arret_travail') {
                try {
                    const response = await sauvegarderFn(event.data.message);
                    // Envoyer la réponse à la popup
                    newWindow.postMessage({
                        action: 'sauvegarde_result',
                        success: response && response.ok,
                        response: response,
                        requestId: event.data.requestId
                    }, '*');
                } catch (error) {
                    // Envoyer l'erreur à la popup
                    newWindow.postMessage({
                        action: 'sauvegarde_result',
                        success: false,
                        error: error.message,
                        requestId: event.data.requestId
                    }, '*');
                }
            }
        });

        // Essayer d'ajouter le bouton plusieurs fois
        setTimeout(ajouterBoutonSauvegarde, 200);
        setTimeout(ajouterBoutonSauvegarde, 500);
        setTimeout(ajouterBoutonSauvegarde, 1000);
        newWindow.addEventListener('load', ajouterBoutonSauvegarde);

    } else {
        console.log("Popup bloquée par le navigateur.");
    }


}

// justification  
function justification() {

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
            <strong><input type="text" value="${nom} ${prenom}" style="width: 180px;"></strong>,
            <span class="editable-field" contenteditable="true" style="min-width: 100px; display: inline-block;">né(e) le ${dob}</span>.
        </p>
        <p>
            est présenter pour la consultation ce jour le 
             <span class="editable-field" contenteditable="true" style="min-width: 120px; display: inline-block;">${todayFormatted}</span> à  l'heure:<input type="time" style="font-size: 11px !important;"> <br> 
            <div style="direction: rtl; text-align: right;">
                 تم فحص المعني يوم :   <span class="editable-field" contenteditable="true" style="min-width: 120px; display: inline-block; margin-left: 10px;">${todayFormatted}</span>   <br>
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






// Fonctions spécifiques pour chaque type de certificat


function ouvrirCertificatProlongation() {

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
    </div>
    <script src="print.js"></script>
    <script src="certificat-unified-font-size.js"></script>

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
    } else {
        console.log("Popup bloquée par le navigateur.");
    }
}



// Version sans poids pour risqueHemorragique
function risqueHemorragiquec2(dateMorsure) {
    const docteur = "Démonstration";

    let date = new Date(dateMorsure);
    date.setFullYear(date.getFullYear() + 1);

    const certificatContent = `
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Roboto:wght@400&display=swap');

        body {
            font-family: 'Roboto', sans-serif;
            background-color: white;
            margin: 0;
            padding: 20px;
        }

        h1, h2, p, label, input, button, span {
            font-family: 'Roboto', sans-serif;
        }

        .header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
        }

        .header img {
            width: 200px;
        }

        .certificat-content {
            border: 2px solid #000;
            padding: 20px;
            margin-bottom: 20px;
        }

        .certificat-content p {
            margin: 10px 0;
        }

        .print-button {
            background-color: #007bff;
            color: white;
            border: none;
            padding: 10px 20px;
            border-radius: 4px;
            cursor: pointer;
            transition: background-color 0.3s ease;
        }

        .print-button:hover {
            background-color: #0056b3;
        }

        .print-button input {
            margin-right: 10px;
            padding: 5px;
            border: 1px solid #ccc;
            border-radius: 4px;
        }
    </style>

    <div class="header">
        <img src="logo.png" alt="Logo">
        <p style="text-align: right;">
            <span class="date">Date de mise à jour:</span>
            <br>
            <span class="date-value">${date.toISOString().split('T')[0]}</span>
        </p>
    </div>

    <div class="certificat-content">
        <p style="text-align: center; font-weight: bold;">CERTIFICAT</p>
        <p style="text-align: center; font-weight: bold;">Dont certificat de non vaccination anti-<em>vaccin cellulaire anti-hémorragique</em> chez un patient piqués par une souris ou une bête féroce</p>
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
    } else {
        console.log("Popup bloquée par le navigateur.");
    }
}

// Fonction pour générer le certificat Avec ATCD Vaccinaux (IM)
function avecATCDVaccinauxIM(dateMorsure, poidsInput) {
    const docteur = "Démonstration";

    let date = new Date(dateMorsure);
    date.setFullYear(date.getFullYear() + 1);

    const certificatContent = `
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Roboto:wght@400&display=swap');

        body {
            font-family: 'Roboto', sans-serif;
            background-color: white;
            margin: 0;
            padding: 20px;
        }

        h1, h2, p, label, input, button, span {
            font-family: 'Roboto', sans-serif;
        }

        .header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
        }

        .header img {
            width: 200px;
        }

        .certificat-content {
            border: 2px solid #000;
            padding: 20px;
            margin-bottom: 20px;
        }

        .certificat-content p {
            margin: 10px 0;
        }

        .print-button {
            background-color: #007bff;
            color: white;
            border: none;
            padding: 10px 20px;
            border-radius: 4px;
            cursor: pointer;
            transition: background-color 0.3s ease;
        }

        .print-button:hover {
            background-color: #0056b3;
        }

        .print-button input {
            margin-right: 10px;
            padding: 5px;
            border: 1px solid #ccc;
            border-radius: 4px;
        }
    </style>

    <div class="header">
        <img src="logo.png" alt="Logo">
        <p style="text-align: right;">
            <span class="date">Date de mise à jour:</span>
            <br>
            <span class="date-value">${date.toISOString().split('T')[0]}</span>
        </p>
    </div>

    <div class="certificat-content">
        <p style="text-align: center; font-weight: bold;">CERTIFICAT</p>
        <p style="text-align: center; font-weight: bold;">Dont certificat de non vaccination anti-<em>vaccin cellulaire anti-hémorragique</em> chez un patient piqués par une souris ou une bête féroce</p>
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
    } else {
        console.log("Popup bloquée par le navigateur.");
    }
}

// Fonctions vaccin cellulaire type 02
function vaccinc2(dateMorsure, poidsInput) {
    const docteur = "Démonstration";

    let date = new Date(dateMorsure);
    date.setFullYear(date.getFullYear() + 1);

    const certificatContent = `
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Roboto:wght@400&display=swap');

        body {
            font-family: 'Roboto', sans-serif;
            background-color: white;
            margin: 0;
            padding: 20px;
        }

        h1, h2, p, label, input, button, span {
            font-family: 'Roboto', sans-serif;
        }

        .header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
        }

        .header img {
            width: 200px;
        }

        .certificat-content {
            border: 2px solid #000;
            padding: 20px;
            margin-bottom: 20px;
        }

        .certificat-content p {
            margin: 10px 0;
        }

        .print-button {
            background-color: #007bff;
            color: white;
            border: none;
            padding: 10px 20px;
            border-radius: 4px;
            cursor: pointer;
            transition: background-color 0.3s ease;
        }

        .print-button:hover {
            background-color: #0056b3;
        }

        .print-button input {
            margin-right: 10px;
            padding: 5px;
            border: 1px solid #ccc;
            border-radius: 4px;
        }
    </style>

    <div class="header">
        <img src="logo.png" alt="Logo">
        <p style="text-align: right;">
            <span class="date">Date de mise à jour:</span>
            <br>
            <span class="date-value">${date.toISOString().split('T')[0]}</span>
        </p>
    </div>

    <div class="certificat-content">
        <p style="text-align: center; font-weight: bold;">CERTIFICAT</p>
        <p style="text-align: center; font-weight: bold;">Dont certificat de non vaccination anti-<em>vaccin cellulaire anti-hémorragique</em> chez un patient piqués par une souris ou une bête féroce</p>
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
    } else {
        console.log("Popup bloquée par le navigateur.");
    }
}

function ouvrirCertificatProlongation() {
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
    <meta http-equiv="Content-Security-Policy" content="script-src 'self' 'unsafe-inline' 'unsafe-eval'; object-src 'none';">
    <title>Certificat de Prolongation</title>
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
        <h1>Certificat de Prolongation</h1>
        <div class="contenu-certificat" style="margin-top: 1.5cm !important;">
        <p>
            Je soussigné, Dr <input type="text" id="docteur" value="${docteur}" placeholder="" style="width: 120px;">, 
            certifie avoir examiné ce jour le(la) susnommé(e) 
            <strong><input type="text" value="${nom} ${prenom}" style="width: 180px;"></strong>,
            <span class="editable-field" contenteditable="true" style="min-width: 100px; display: inline-block;">né(e) le ${dob}</span>.
        </p>
        <p>
            Déclare que son état de santé nécessite une prolongation d'arrêt de travail de
            <input type="text" placeholder="1 (un)" style="width: 70px;"> Jour(s)
            à dater du <span class="editable-field" contenteditable="true" style="min-width: 120px; display: inline-block;">${todayFormatted}</span> sauf complication.
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

    const newWindow = window.open("", "_blank");
    if (newWindow) {
        newWindow.document.write(certificatContent);
        newWindow.document.close();

        // Ajouter le bouton de sauvegarde après que le document soit chargé
        function ajouterBoutonSauvegarde() {
            try {
                const printButton = newWindow.document.getElementById('printButton');
                const saveButton = newWindow.document.getElementById('sauvegarderProPopup');

                if (printButton && !saveButton) {
                    console.log('✅ Ajout du bouton de sauvegarde dans la popup');

                    // Créer le bouton de sauvegarde
                    const boutonSauvegarde = newWindow.document.createElement('button');
                    boutonSauvegarde.id = 'sauvegarderProPopup';
                    boutonSauvegarde.innerHTML = '💾 Sauvegarder Prolon';
                    boutonSauvegarde.style.cssText = 'background-color: #28a745; color: white; border: none; padding: 10px 20px; margin-left: 15px; border-radius: 5px; cursor: pointer; font-size: 16px; transition: background-color 0.3s;';

                    // Ajouter l'effet hover
                    boutonSauvegarde.addEventListener('mouseenter', function () {
                        this.style.backgroundColor = '#218838';
                    });
                    boutonSauvegarde.addEventListener('mouseleave', function () {
                        this.style.backgroundColor = '#28a745';
                    });

                    // Ajouter le bouton à côté du bouton d'impression
                    printButton.parentNode.appendChild(boutonSauvegarde);

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
                        // Prendre le texte complet sans validation de format
                        const editableFields = newWindow.document.querySelectorAll('.editable-field');
                        let dateNaissance = '';
                        for (let field of editableFields) {
                            const text = field.textContent || field.innerText || '';
                            // Chercher le champ qui contient "né(e) le" ou similaire
                            const parentText = field.parentElement ? field.parentElement.textContent || '' : '';
                            if (parentText.includes('né(e)') || parentText.includes('né') || parentText.includes('née')) {
                                // Prendre tout le texte du champ sans validation
                                dateNaissance = text.trim();
                                break;
                            }
                        }

                        // Si pas trouvé, prendre le premier champ editable-field
                        if (!dateNaissance && editableFields.length > 0) {
                            dateNaissance = (editableFields[0].textContent || editableFields[0].innerText || '').trim();
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

                        console.log('📤 Message à envoyer:', message);

                        // Utiliser directement la fonction stockée dans la popup
                        const sauvegarderFn = window.sauvegarderArretTravailDepuisPopup;

                        if (sauvegarderFn && typeof sauvegarderFn === 'function') {
                            sauvegarderFn(message).then(response => {
                                if (response && response.ok) {
                                    afficherMessageDansPopup('prolongation sauvegardé avec succès !', 'success');
                                } else {
                                    const errorMsg = response ? response.error : 'Réponse invalide';
                                    afficherMessageDansPopup('Erreur lors de la sauvegarde: ' + errorMsg, 'error');
                                }
                            }).catch(error => {
                                console.error('❌ Erreur lors de la sauvegarde:', error);
                                afficherMessageDansPopup('Erreur lors de la sauvegarde: ' + error.message, 'error');
                            });
                        } else {
                            // Si la fonction n'est pas encore disponible, attendre un peu
                            setTimeout(() => {
                                const fn = window.sauvegarderArretTravailDepuisPopup;
                                if (fn && typeof fn === 'function') {
                                    fn(message).then(response => {
                                        if (response && response.ok) {
                                            afficherMessageDansPopup('prolongation sauvegardé avec succès !', 'success');
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

                    // Ajouter l'événement de sauvegarde
                    boutonSauvegarde.addEventListener('click', function () {
                        console.log('💾 Clic sur le bouton de sauvegarde');

                        // Récupérer le nombre de jours
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
                            // Créer un prompt personnalisé dans la popup
                            const promptDiv = newWindow.document.createElement('div');
                            promptDiv.id = 'promptNombreJours';
                            promptDiv.style.cssText = 'position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%); z-index: 10001; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3); min-width: 300px;';
                            promptDiv.innerHTML = '<div style="margin-bottom: 15px; font-weight: bold; font-size: 16px;">Nombre de jours d\'arrêt de travail</div><input type="number" id="inputNombreJours" value="1" min="1" style="width: 100%; padding: 8px; border: 1px solid #ccc; border-radius: 4px; font-size: 14px; margin-bottom: 15px;"><div style="display: flex; gap: 10px; justify-content: flex-end;"><button id="btnPromptOk" style="padding: 8px 20px; background: #28a745; color: white; border: none; border-radius: 4px; cursor: pointer;">OK</button><button id="btnPromptCancel" style="padding: 8px 20px; background: #6c757d; color: white; border: none; border-radius: 4px; cursor: pointer;">Annuler</button></div>';

                            // Créer un overlay
                            const overlay = newWindow.document.createElement('div');
                            overlay.id = 'promptOverlay';
                            overlay.style.cssText = 'position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0, 0, 0, 0.5); z-index: 10000;';

                            newWindow.document.body.appendChild(overlay);
                            newWindow.document.body.appendChild(promptDiv);

                            // Focus sur l'input
                            setTimeout(() => {
                                const input = newWindow.document.getElementById('inputNombreJours');
                                if (input) {
                                    input.focus();
                                    input.select();
                                }
                            }, 100);

                            // Gérer les boutons
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

                            // Gérer Enter et Escape
                            const input = newWindow.document.getElementById('inputNombreJours');
                            input.addEventListener('keydown', (e) => {
                                if (e.key === 'Enter') {
                                    btnOk.click();
                                } else if (e.key === 'Escape') {
                                    btnCancel.click();
                                }
                            });
                        } else {
                            // Vérifier que le nombre de jours est valide
                            if (!nombreJours || isNaN(nombreJours) || parseInt(nombreJours) <= 0) {
                                afficherMessageDansPopup('Veuillez entrer un nombre de jours valide', 'warning');
                                return;
                            }
                            // Effectuer la sauvegarde directement
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

            // Utiliser la fonction envoyerMessageNatif si disponible
            if (typeof envoyerMessageNatif === 'function') {
                return await envoyerMessageNatif(message);
            } else if (typeof browser !== 'undefined' && browser.runtime && browser.runtime.sendNativeMessage) {
                return await browser.runtime.sendNativeMessage("com.daoudi.certificat", message);
            } else if (typeof chrome !== 'undefined' && chrome.runtime && chrome.runtime.sendNativeMessage) {
                return await new Promise((resolve, reject) => {
                    chrome.runtime.sendNativeMessage("com.daoudi.certificat", message, (response) => {
                        if (chrome.runtime.lastError) {
                            reject(new Error(chrome.runtime.lastError.message));
                        } else {
                            resolve(response);
                        }
                    });
                });
            } else {
                throw new Error('Aucune API de messagerie native disponible');
            }
        };

        // Exposer la fonction dans la fenêtre parent
        window.sauvegarderArretTravailDepuisPopup = sauvegarderFn;

        // Stocker la référence à la fenêtre parent dans la popup pour qu'elle soit accessible
        newWindow._parentWindowRef = window;

        // Créer la fonction wrapper directement dans la popup après que le document soit chargé
        // Cette fonction utilisera postMessage pour communiquer avec la fenêtre parent
        function setupSauvegardeFunction() {
            newWindow.sauvegarderArretTravailDepuisPopup = function (message) {
                return new Promise((resolve, reject) => {
                    // Utiliser la référence stockée à la fenêtre parent
                    const parentWin = newWindow._parentWindowRef;
                    if (!parentWin) {
                        reject(new Error('Fenêtre parent non accessible'));
                        return;
                    }

                    // Générer un ID unique pour cette requête
                    const requestId = Date.now() + Math.random();

                    // Écouter la réponse
                    const messageHandler = function (event) {
                        if (event.data && event.data.action === 'sauvegarde_result' && event.data.requestId === requestId) {
                            newWindow.removeEventListener('message', messageHandler);
                            if (event.data.success) {
                                resolve(event.data.response);
                            } else {
                                reject(new Error(event.data.error || 'Erreur inconnue'));
                            }
                        }
                    };
                    newWindow.addEventListener('message', messageHandler);

                    // Envoyer la requête à la fenêtre parent
                    parentWin.postMessage({
                        action: 'sauvegarder_arret_travail',
                        message: message,
                        requestId: requestId
                    }, '*');

                    // Timeout après 10 secondes
                    setTimeout(() => {
                        newWindow.removeEventListener('message', messageHandler);
                        reject(new Error('Timeout: La sauvegarde prend trop de temps'));
                    }, 10000);
                });
            };
        }

        // Configurer la fonction après que le document soit chargé
        newWindow.addEventListener('load', setupSauvegardeFunction);

        // Aussi essayer immédiatement au cas où le document est déjà chargé
        if (newWindow.document.readyState === 'complete') {
            setupSauvegardeFunction();
        }

        // Écouter les messages de la popup dans la fenêtre parent
        window.addEventListener('message', async function (event) {
            // Vérifier que le message vient de la popup
            if (event.source === newWindow && event.data && event.data.action === 'sauvegarder_arret_travail') {
                try {
                    const response = await sauvegarderFn(event.data.message);
                    // Envoyer la réponse à la popup
                    newWindow.postMessage({
                        action: 'sauvegarde_result',
                        success: response && response.ok,
                        response: response,
                        requestId: event.data.requestId
                    }, '*');
                } catch (error) {
                    // Envoyer l'erreur à la popup
                    newWindow.postMessage({
                        action: 'sauvegarde_result',
                        success: false,
                        error: error.message,
                        requestId: event.data.requestId
                    }, '*');
                }
            }
        });

        // Essayer d'ajouter le bouton plusieurs fois
        setTimeout(ajouterBoutonSauvegarde, 200);
        setTimeout(ajouterBoutonSauvegarde, 500);
        setTimeout(ajouterBoutonSauvegarde, 1000);
        newWindow.addEventListener('load', ajouterBoutonSauvegarde);

    } else {
        console.log("Popup bloquée par le navigateur.");
    }
}

function ouvrirCertificatRadiox() {
    // Vider uniquement les champs de consultation et type d'exploration
    localStorage.removeItem('raisonConsultation');
    localStorage.removeItem('typeExploration');

    const { nom, prenom, dob } = patientInfo;
    const docteur = localStorage.getItem('docteur') || "";

    const today = new Date();
    const year = today.getFullYear();
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const day = String(today.getDate()).padStart(2, '0');
    const todayFormatted = `${year}-${month}-${day}`;

    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';
    let enteteContent = avecEntete ? generateHeader() : '<div style="height: 155px;"></div>';



    const certificatContent = `
    <!DOCTYPE html>
    <html lang="fr">
    <head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Radiox</title>
    <style>
    /* Général */
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
    .search-box {
        width: 200px;
        padding: 8px;
        border: 1px solid #ddd;
        border-radius: 4px;
        margin-right: 10px;
    }
    .lock-button {
        background: none;
        border: none;
        cursor: pointer;
        padding: 0;
        margin-left: 10px;
    }
    .lock-button:hover {
        opacity: 0.8;
    }
    #suggestions {
        position: absolute;
        background: white;
        border: 1px solid #ddd;
        border-radius: 4px;
        padding: 5px;
        margin-top: 5px;
        max-height: 200px;
        overflow-y: auto;
        width: 200px;
        z-index: 1000;
    }
    .suggestion-item {
        padding: 5px;
        cursor: pointer;
        border-bottom: 1px solid #eee;
    }
    .suggestion-item:hover {
        background-color: #f0f0f0;
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

        input::placeholder,
        textarea::placeholder {
            color: transparent !important;
        }

        .print-button {
            display: none;
        }

        .editable-area {
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
        <h1>Cher confrère</h1>
        <div class="contenu-certificat" style="margin-top: 1.5cm !important;">
        <p>
            Permettez-moi de vous adresser le(a) nommé(e) <strong><input type="text" value="${nom} ${prenom}" style="width: 180px;" id="patientNomPrenom"></strong>,
            né(e) le <strong><input type="text" value="${dob}" style="width: 120px;" id="patientDateNaissance"></strong>, qui consulte chez nous pour :<br>
            <textarea id="raisonConsultation" class="editable-area" placeholder="Raison de la consultation"></textarea>
            
            Pour faire un :<br>
            <textarea id="typeExploration" class="editable-area" placeholder="Type d'exploration"></textarea>
        </p>
        <p style="text-align: right; margin-right: 50px;">
            Confraternellement,<br>
            <span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>
        </p>
        </div>
    </div>
    <div class="print-button">
        <button id="printButton">Imprimer la lettre</button>
    </div>
	
    <script src="search.js"></script>
    <script src="print.js"></script>
    <script src="modify-radiox-script.js"></script>
    <script src="radiox-scripts.js"></script>
    <script>
        // Sauvegarder les modifications dans le localStorage
        document.addEventListener('DOMContentLoaded', function() {
            // Sauvegarder les champs de base
            const fields = ['patientNomPrenom', 'patientDateNaissance', 'raisonConsultation', 'typeExploration'];
            fields.forEach(id => {
                const element = document.getElementById(id);
                if (element) {
                    // Restaurer la valeur sauvegardée
                    const savedValue = localStorage.getItem(id);
                    if (savedValue) {
                        element.value = savedValue;
                    }
                    // Ajouter l'écouteur d'événement
                    element.addEventListener('input', function() {
                        localStorage.setItem(id, this.value);
                    });
                }
            });
        });
    </script>
    </body>
    </html>
    `;

    const newWindow = window.open("", "_blank");
    if (newWindow) {
        newWindow.document.write(certificatContent);
        newWindow.document.close();
    } else {
        console.log("Popup bloquée par le navigateur.");
    }
}



// lettre 
function ouvrirCertificatLettre() {
    // Vider les champs de recherche dans le stockage local
    localStorage.removeItem('searchInput');
    localStorage.removeItem('searchInput2');
    localStorage.removeItem('searchInput3');
    localStorage.removeItem('searchInput4');

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
<title>Cher confrère</title>
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
<h1>Cher confrère</h1>
<p>
Permettez-moi de vous adresser le(a) sus nommé(e)
<strong><input type="text" value="${nom} ${prenom}" class="editable-input"></strong>,
né(e) le <strong><input type="text" value="${dob}" class="editable-input"></strong>,
qui consulte chez nous pour :<br>
<input type="text" id="searchInput" placeholder="Raison de la consultation" class="full-width-input">

<div class="optional-field">
<input type="text" value="Il(elle) a comme ATCD: " class="label-input">
<input type="text" id="searchInput2" placeholder="Antécédents médicaux" class="full-width-input">
</div>

<div class="optional-field">
<input type="text" value="L'examen clinique présent: " class="label-input">
<input type="text" id="searchInput3" placeholder="Examen clinique" class="full-width-input">
</div>

<div class="optional-field">
<input type="text" value="Qui fait évoquer: " class="label-input">
<input type="text" id="searchInput4" placeholder="Diagnostic" class="full-width-input">
</div>

<p>Je vous le(la) confie pour avis et éventuelle prise en charge spécialisée.</p>
</p>
<p style="text-align: right; margin-right: 50px;">
Confraternellement&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp<br>
<span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp;
</p>
</div>

<style>
.editable-input {
border: 1px solid #ddd;
padding: 2px 5px;
margin: 0 3px;
min-width: 100px;
}
.full-width-input {
width: 100%;
border: 1px solid #ddd;
padding: 8px;
margin: 5px 0;
box-sizing: border-box;
}
.label-input {
border: none;
background: none;
font-weight: bold;
padding: 2px 5px;
margin-right: 5px;
}
.optional-field {
margin: 10px 0;
}

@media print {
input {
border: none !important;
background: none !important;
}
.label-input {
padding: 0 !important;
}
input::placeholder {
color: transparent; /* Rendre le placeholder transparent lors de l'impression */
}
}
</style>
<div class="print-button" style="display: flex; align-items: center; justify-content: center; gap: 15px;">
    <div style="display: flex; align-items: center; gap: 8px;">
        <label for="fontSize" style="font-size: 14px; margin: 0;">Taille du texte:</label>
        <input type="number" id="fontSize" min="8" max="20" value="14" style="width: 60px; padding: 5px; border: 1px solid #bdbdbd; border-radius: 4px;">
    </div>
    <button id="printButton">Imprimer la lettre</button>
</div>
<script src="search.js"></script>
<script src="print.js"></script>
<script src="certificat-unified-font-size.js"></script>
<script src="lettre-scripts.js"></script>
<script>
// Initialisation des champs de la lettre
document.addEventListener('DOMContentLoaded', function() {
    // Gestion des champs de la polyclinique et du docteur
    const polycliniqueInput = document.getElementById('polyclinique');
    const polycliniqueArInput = document.getElementById('polyclinique-ar');
    const docteurInput = document.querySelector('[contenteditable]');

    if (polycliniqueInput) {
        polycliniqueInput.addEventListener('input', function() {
            localStorage.setItem('polyclinique', this.value);
        });
    }

    if (polycliniqueArInput) {
        polycliniqueArInput.addEventListener('input', function() {
            localStorage.setItem('polyclinique-ar', this.value);
        });
    }

    if (docteurInput) {
        docteurInput.addEventListener('input', function() {
            localStorage.setItem('docteur', this.textContent);
        });
    }

    // Adaptation automatique de la hauteur
    const editableAreas = document.querySelectorAll('.editable-area');
    editableAreas.forEach(area => {
        area.style.height = 'auto';
        area.style.height = (area.scrollHeight) + 'px';

        area.addEventListener('input', function() {
            this.style.height = 'auto';
            this.style.height = (this.scrollHeight) + 'px';
        });
    });
});
</script>
</body>
</html>
`;

    const newWindow = window.open("", "_blank");
    if (newWindow) {
        newWindow.document.write(certificatContent);
        newWindow.document.close();
    } else {
        console.error("Impossible d'ouvrir une nouvelle fenetre. Veuillez vérifier les paramètres de blocage des fenetres popup.");
    }
}


//Certificat Chronique
function ouvrirCertificatChronique() {
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
<title>Chronique</title>
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
}
</style>
</head>
<body>
${enteteContent}
<div class="certificat">
<h1>Certificat médical de maladie chronique</h1>
<br>
<p>
Je soussigné, Dr <input type="text" id="docteur" value="${docteur}" placeholder="Medecin">, certifie que
le(a) nommé(e) <strong><input type="text" value="${nom} ${prenom}" style="width: 180px;"></strong>,
<span class="editable-field" contenteditable="true" style="min-width: 100px; display: inline-block;">né(e) le ${dob}</span>.<br>
Présente une maladie chronique de type :<br>
<textarea placeholder="Type de maladie chronique" style="width: 100%;"></textarea><br>
Depuis :<br>
<input type="text" placeholder="Date de début"><br>
Nécessitant un traitement à  long cours à  base de : <br>
<textarea placeholder="Traitement" style="width: 100%;"></textarea><br>
Ce certificat est établi sur les renseignements fournis par le(a) patient(e) et délivré à  la demande de l'intéressé(e) pour servir et valoir ce que de droit.<br>

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

//reprise de travail 
function ouvrirCertificatReprise() {
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




// cbv 
function cbv() {

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
            <strong><input type="text" value="${nom} ${prenom}" style="width: 180px;"></strong>,
            <span class="editable-field" contenteditable="true" style="min-width: 100px; display: inline-block;">né(e) le ${dob}</span>.
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
             le <span class="editable-field" contenteditable="true" style="min-width: 120px; display: inline-block;">${todayFormatted}</span> à  l'heure:<input type="time" style="font-size: 11px !important;"> <br> 
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
        const heureInput = certificatWindow.document.querySelector('input[type="time"]');
        const heure = heureInput ? heureInput.value : '';

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

        // Préparer le message pour l'application native
        const message = {
            action: "ajouter_cbv",
            nom: nom,
            prenom: prenom,
            medecin: medecin,
            date_certificat: dateCertificat,
            date_naissance: dateNaissance || null,
            titre: titre || null,
            examen: examen || null,
            heure: heure || null
        };

        console.log('📤 Message à envoyer:', message);

        // Envoyer à l'application native using the CBV-specific function
        if (typeof envoyerMessageNatif !== 'undefined') {
            await envoyerMessageNatif(message);
        } else {
            // Fallback: try to send directly
            try {
                const response = await browser.runtime.sendNativeMessage("com.daoudi.certificat", message);
                if (response && response.ok) {
                    console.log('✅ CBV sauvegardé avec succès');
                    alert('✅  sauvegardé avec succès !');
                } else {
                    const errorMsg = response ? response.error : 'Réponse invalide';
                    console.error('❌ Erreur de sauvegarde:', errorMsg);
                    alert('❌ Erreur lors de la sauvegarde: ' + errorMsg);
                }
            } catch (error) {
                console.error('❌ Erreur de communication native:', error);
                alert('❌ Erreur de communication avec l\'application native: ' + error.message);
                throw error;
            }
        }

    } catch (error) {
        console.error('❌ Erreur lors de la sauvegarde CBV:', error);
        alert('Erreur lors de la sauvegarde: ' + error.message);
    }
}




//requisition	
function requisition() {
    const { nom, prenom, dob } = patientInfo;

    const polyclinique = localStorage.getItem('polyclinique') || "";
    const docteur = localStorage.getItem('docteur') || "";

    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';
    const enteteContent = avecEntete ? generateHeader() : '<div style="height: 155px;"></div>';

    // Définir d'abord les fonctions globales
    window.openCertificat = function (type, raison = "") {
        const today = new Date().toLocaleDateString('fr-FR');
        let certificatText = "";

        if (type === "apte") {
            certificatText = `
                <p>
                    Je soussigné(e), Dr <strong>${docteur}</strong>, certifie avoir examiné ce jour
                    <strong>${nom} ${prenom}</strong>, né(e) le <strong>${dob}</strong>.
                </p>
                <p>Ne présente aucune contre-indication à  une mesure de garde à  vue.</p>
            `;
        } else {
            certificatText = `
                <p>
                    Je soussigné(e), Dr <strong>${docteur}</strong>, certifie avoir examiné ce jour
                    <strong>${nom} ${prenom}</strong>, né(e) le <strong>${dob}</strong>.
                </p>
                <p>Présente des contre-indications à  la garde à  vue pour les raisons suivantes :</p>
                <p><strong>${raison}</strong></p>
                <p>En conséquence, je recommande qu'il/elle ne soit pas soumis(e) à  la garde à  vue.</p>
            `;
        }

        const certWindow = window.open("", "_blank");
        certWindow.document.write(`
            <html>
            <head>
                <meta charset="UTF-8">
                <title>Certificat</title>
                <style>
                    body {
                        font-family: Arial, sans-serif;
                        padding: 30px;
                        background-color: #fff;
                    }
                    h1 {
                        text-align: center;
                        text-decoration: underline;
                        font-size: 20px;
                    }
                    p {
                        line-height: 1.6;
                        font-size: 16px;
                    }
                    @media print {
                        button { display: none; }
                        h1 {
                            margin-top: 2cm !important;
                        }
                    }
                </style>
            </head>
            <body>
                ${enteteContent}
                <h1>CERTIFICAT MEDICAL</h1>
                ${certificatText}
                <p style="text-align:right; margin-top:30px;">Fait le ${today}</p>
                <p style="text-align:right;"><strong>Dr ${docteur}</strong></p>
                <div style="text-align:center; margin-top:30px;">
                    <button id="printButton2">Imprimer</button>
                </div>
                <script>
                document.getElementById('printButton2').addEventListener('click', function() {
                    window.print();
                });
                </script>
            </body>
            </html>
        `);
        certWindow.document.close();
    };

    window.askReason = function () {
        const reasonWindow = window.open("", "_blank", "width=400,height=300");
        reasonWindow.document.write(`
            <html>
            <head>
                <title>Motif d'inaptitude</title>
                <style>
                    body {
                        font-family: Arial, sans-serif;
                        display: flex;
                        flex-direction: column;
                        justify-content: center;
                        align-items: center;
                        height: 100vh;
                        padding: 20px;
                    }
                    textarea {
                        width: 80%;
                        height: 100px;
                        margin-bottom: 20px;
                        font-size: 16px;
                        padding: 10px;
                    }
                    button {
                        padding: 10px 20px;
                        font-size: 16px;
                        cursor: pointer;
                    }
                </style>
            </head>
            <body>
                <h3>Veuillez saisir la raison de l'inaptitude :</h3>
                <textarea id="raison" placeholder="Exemple : Trouble psychiatrique, blessure, etc."></textarea>
                <button id="submitReasonButton">Valider</button>

                <script>
                    function submitReason() {
                        const raison = document.getElementById('raison').value;
                        if (raison.trim() === "") {
                            alert("Veuillez saisir une raison.");
                            return;
                        }
                        window.opener.openCertificat('inapte', raison);
                        window.close();
                    }
                    
                    document.getElementById('submitReasonButton').addEventListener('click', function() {
                        submitReason();
                    });
                </script>
            </body>
            </html>
        `);
        reasonWindow.document.close();
    };



}






// bilan Leishmaniose

function ouvrirCertificatLeishmaniose() {
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



//cat Leishmaniose inf a 03 lesions

function ouvrirCertificatLeishmanioseDetail() {
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
Infiltrations locales pour le(a) nommé(e) <br><strong><input type="text" value="${nom} ${prenom}" style="width: 200px;"></strong>  de <input type="text" value="02" size="2" />cc du Glucantime<br>
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
Permettez moi de vous adresser le(a) nommé(e) <strong><input type="text" value="${nom} ${prenom}" style="width: 200px;"></strong>  <br>
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
// Fonctions vaccin cellulaire type 02
// Fonction pour le schéma Zagreb
function zegreb() {


    // Obtenir la date d'aujourd'hui au format YYYY-MM-DD
    const today = new Date();
    const year = today.getFullYear();
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const day = String(today.getDate()).padStart(2, '0');
    const todayFormatted = `${year}-${month}-${day}`;

    // Créer une modale pour demander la date de morsure
    const modalContent = `
    <div style="background: white; padding: 20px; border-radius: 5px; text-align: center;">
        <h3>Schéma Zagreb</h3>
        <p>Veuillez entrer la date du 01er jour de vaccination (Jour 0) :</p>
        <input type="date" id="dateMorsure" value="${todayFormatted}" style="padding: 8px; margin: 10px 0;">
        <div>
            <button id="confirmDate" style="padding: 8px 16px; background-color: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">Confirmer</button>
        </div>
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
    modal.style.zIndex = '1000';
    modal.innerHTML = modalContent;

    document.body.appendChild(modal);

    // Ecouteur pour le bouton de confirmation
    modal.querySelector('#confirmDate').addEventListener('click', function () {
        const dateMorsureInput = modal.querySelector('#dateMorsure');
        if (!dateMorsureInput.value) {
            alert("Veuillez entrer une date de morsure valide.");
            return;
        }

        // Fermer la modale
        document.body.removeChild(modal);

        // Appeler la fonction originale avec la date de morsure
        generateZagrebCertificat(dateMorsureInput.value);
    });

    // Fermer la modale si l'utilisateur clique en dehors
    modal.addEventListener('click', function (e) {
        if (e.target === modal) {
            document.body.removeChild(modal);
        }
    });
}

// Fonction pour générer le certificat Zagreb avec la date de morsure
function generateZagrebCertificat(dateMorsure) {

    const { nom, prenom, dob } = patientInfo;

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



    // Open the Zagreb certificate HTML file
    const zagrebWindow = window.open(browser.runtime.getURL('zagreb-certificat.html'), '_blank');

    // Pass data to the new window when it loads
    zagrebWindow.onload = function () {
        // Pass data to the new window
        zagrebWindow.certData = {
            nom: nom,
            prenom: prenom,
            dob: dob,
            docteur: docteur,
            dateFormattedJour0: dateFormattedJour0,
            dateFormattedPlus7: dateFormattedPlus7,
            dateFormattedPlus21: dateFormattedPlus21,
            enteteContent: enteteContent
        };

        // Dispatch a custom event to notify the page that data is ready
        zagrebWindow.dispatchEvent(new Event('certDataReady'));
    };

    // Return early to avoid executing the old code
    return;
}

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

// Fonctions vaccin cellulaire type 02 essens
function essens() {
    // Obtenir la date d'aujourd'hui au format YYYY-MM-DD
    const today = new Date();
    const year = today.getFullYear();
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const day = String(today.getDate()).padStart(2, '0');
    const todayFormatted = `${year}-${month}-${day}`;

    // Créer une modale pour demander la date de morsure
    const modalContent = `
    <div style="background: white; padding: 20px; border-radius: 5px; text-align: center;">
        <h3>Schéma Essen</h3>
        <p>Veuillez entrer la date du 01er jour de vaccination (Jour 0) :</p>
        <input type="date" id="dateMorsure" value="${todayFormatted}" style="padding: 8px; margin: 10px 0;">
        <div>
            <button id="confirmDate" style="padding: 8px 16px; background-color: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">Confirmer</button>
        </div>
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
    modal.style.zIndex = '1000';
    modal.innerHTML = modalContent;

    document.body.appendChild(modal);

    // Ecouteur pour le bouton de confirmation
    modal.querySelector('#confirmDate').addEventListener('click', function () {
        const dateMorsureInput = modal.querySelector('#dateMorsure');
        if (!dateMorsureInput.value) {
            alert("Veuillez entrer une date de morsure valide.");
            return;
        }

        // Fermer la modale
        document.body.removeChild(modal);

        // Appeler la fonction originale avec la date de morsure
        generateEssenCertificat(dateMorsureInput.value);
    });

    // Fermer la modale si l'utilisateur clique en dehors
    modal.addEventListener('click', function (e) {
        if (e.target === modal) {
            document.body.removeChild(modal);
        }
    });
}

// Fonction pour générer le certificat Essen avec la date de morsure
function generateEssenCertificat(dateMorsure) {
    const nom = localStorage.getItem('nom') || "";
    const prenom = localStorage.getItem('prenom') || "";
    const dob = localStorage.getItem('dob') || "";

    // Calcul des dates selon le schéma de Essen
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

    // Open the Essen certificate HTML file
    const essenWindow = window.open(browser.runtime.getURL('essen-certificat.html'), '_blank');

    // Pass data to the new window when it loads
    essenWindow.onload = function () {
        // Pass data to the new window
        essenWindow.certData = {
            nom: nom,
            prenom: prenom,
            dob: dob,
            docteur: docteur,
            dateFormattedJour0: dateFormattedJour0,
            dateFormattedPlus3: dateFormattedPlus3,
            dateFormattedPlus7: dateFormattedPlus7,
            dateFormattedPlus14: dateFormattedPlus14,
            enteteContent: enteteContent
        };

        // Dispatch a custom event to notify the page that data is ready
        essenWindow.dispatchEvent(new Event('certDataReady'));
    };

    // Return early to avoid executing the old code
    return;
}

// Fonctions vaccin Tissulaire sans sar
// Fonction pour le schéma Tissulaire sans SAR
function Tissulairesanssar() {
    // Créer une modale pour demander la date de morsure

    const today = new Date();
    const year = today.getFullYear();
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const day = String(today.getDate()).padStart(2, '0');
    const todayFormatted = `${year}-${month}-${day}`;

    const modalContent = `
<div style="background: white; padding: 20px; border-radius: 5px; text-align: center;">
<h3>Schéma Tissulaire sans SAR</h3>
<p>Veuillez entrer la date du 01er jour de vaccination (Jour 0) :</p>
<input type="date" id="dateMorsure" value="${todayFormatted}" style="padding: 8px; margin: 10px 0;">
<div>
<button id="confirmDate" style="padding: 8px 16px; background-color: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">Confirmer</button>
</div>
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
    modal.style.zIndex = '1000';
    modal.innerHTML = modalContent;

    document.body.appendChild(modal);

    // Ecouteur pour le bouton de confirmation
    modal.querySelector('#confirmDate').addEventListener('click', function () {
        const dateMorsureInput = modal.querySelector('#dateMorsure');
        if (!dateMorsureInput.value) {
            alert("Veuillez entrer une date de morsure valide.");
            return;
        }

        // Fermer la modale
        document.body.removeChild(modal);

        // Préparer les données pour le certificat
        const certData = {
            nom: patientInfo.nom || '',
            prenom: patientInfo.prenom || '',
            dob: patientInfo.dob || '',
            docteur: localStorage.getItem('docteur') || '',
            dateFormattedJour0: dateMorsureInput.value,
            dateFormattedPlus1: formatDate(new Date(new Date(dateMorsureInput.value).getTime() + 1 * 24 * 60 * 60 * 1000)),
            dateFormattedPlus2: formatDate(new Date(new Date(dateMorsureInput.value).getTime() + 2 * 24 * 60 * 60 * 1000)),
            dateFormattedPlus3: formatDate(new Date(new Date(dateMorsureInput.value).getTime() + 3 * 24 * 60 * 60 * 1000)),
            dateFormattedPlus4: formatDate(new Date(new Date(dateMorsureInput.value).getTime() + 4 * 24 * 60 * 60 * 1000)),
            dateFormattedPlus5: formatDate(new Date(new Date(dateMorsureInput.value).getTime() + 5 * 24 * 60 * 60 * 1000)),
            dateFormattedPlus6: formatDate(new Date(new Date(dateMorsureInput.value).getTime() + 6 * 24 * 60 * 60 * 1000)),
            dateFormattedPlus10: formatDate(new Date(new Date(dateMorsureInput.value).getTime() + 10 * 24 * 60 * 60 * 1000)),
            dateFormattedPlus14: formatDate(new Date(new Date(dateMorsureInput.value).getTime() + 14 * 24 * 60 * 60 * 1000)),
            dateFormattedPlus29: formatDate(new Date(new Date(dateMorsureInput.value).getTime() + 29 * 24 * 60 * 60 * 1000)),
            dateFormattedPlus90: formatDate(new Date(new Date(dateMorsureInput.value).getTime() + 90 * 24 * 60 * 60 * 1000)),
            enteteContent: localStorage.getItem('certificatFormat') === 'avecEntete' ? generateHeader() : '<div style="height: 150px;"></div>'
        };

        // Ouvrir le certificat dans un nouvel onglet
        chrome.tabs.create({
            url: chrome.runtime.getURL('schema-tissulaire-sans-sar-certificat.html'),
            active: true
        }, function (tab) {
            // Attendre que l'onglet soit chargé pour envoyer les données
            chrome.tabs.onUpdated.addListener(function listener(tabId, changeInfo, tab) {
                if (tabId === tab.id && changeInfo.status === 'complete') {
                    // Envoyer les données au certificat
                    chrome.tabs.sendMessage(tabId, { certData: certData });
                    // Retirer l'écouteur
                    chrome.tabs.onUpdated.removeListener(listener);
                }
            });
        });
    });

    // Fermer la modale si l'utilisateur clique en dehors
    modal.addEventListener('click', function (e) {
        if (e.target === modal) {
            document.body.removeChild(modal);
        }
    });
}

// Fonction pour formater la date
function formatDate(date) {
    return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
}

// Fonction pour générer le certificat Tissulaire sans SAR


// Fonction pour générer le certificat Tissulaire sans SAR
function generateTissulaireSansSARCertificat(dateMorsure) {
    const { nom, prenom, dob } = patientInfo;
    const animal = "chien"; // Animal en cause (valeur par défaut)

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

    // Formatage des dates
    const formatDate = (date) => `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;

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
        enteteContent = '<div style="height: 150px;"></div>';
    }



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
margin-top: 05px;
box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
}
h1 {
text-align: center;
color: #333;
text-decoration: underline;
font-size: 15px;
margin-top: 05px;
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
}
</style>
</head>
<body>
${enteteContent}

<div class="certificat">
<h1>Schéma Antirabique - Tissulaire sans SAR</h1>
<p>
Nom et Prenom : <strong><input type="text" value="${nom} ${prenom}" style="width: auto;"></strong><br>
Date de naissance : <strong><input type="text" value="${dob}" style="width: auto;"></strong><br>
Animal en cause : <strong><input type="text" value="${animal}" style="width: auto;"></strong><br>
Classe 02, schéma choisi : vaccin tissulaire / sans SAR<br>

Jour 0 : <input type="date" id="dateJour0" value="${formatDate(dateJour0)}" readonly>( dans les 07 premieres jous les inj s/cutanee péri ombilicale)<br>
Jour 1 : <input type="date" id="dateJour1" value="${formatDate(dateJour1)}" readonly><br>
Jour 2 : <input type="date" id="dateJour2" value="${formatDate(dateJour2)}" readonly><br>
Jour 3 : <input type="date" id="dateJour3" value="${formatDate(dateJour3)}" readonly><br>
Jour 4 : <input type="date" id="dateJour4" value="${formatDate(dateJour4)}" readonly><br>
Jour 5 : <input type="date" id="dateJour5" value="${formatDate(dateJour5)}" readonly><br>
Jour 6 : <input type="date" id="dateJour6" value="${formatDate(dateJour6)}" readonly><br>
================== les rappeles en ID dans dans les av bras=============<br>
Jour 10 : <input type="date" id="dateJour10" value="${formatDate(dateJour10)}" readonly><br>
Jour 14 : <input type="date" id="dateJour14" value="${formatDate(dateJour14)}" readonly><br>
Jour 29 : <input type="date" id="dateJour29" value="${formatDate(dateJour29)}" readonly><br>
Jour 90 : <input type="date" id="dateJour90" value="${formatDate(dateJour90)}" readonly><br>
<br>
en cas d'âge <05ans la dose sera 1/2 amp( 01 ml)<br>
</p>
<p style="text-align: right; margin-top: 15px;">
Medecin traitant <br>
<span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>&nbsp&nbsp&nbsp&nbsp&nbsp;
</p>
</div>

<div class="print-button">
<button id="printButton">Imprimer le schéma</button>
<button id="saveButtonRc2">sauvegarder</button>
</div>
<script src="print.js"></script>

<script>
document.addEventListener('DOMContentLoaded', function () {
document.getElementById('polyclinique').addEventListener('input', function () {
localStorage.setItem('polyclinique', this.value);
});

document.getElementById('polyclinique-ar').addEventListener('input', function () {
localStorage.setItem('polyclinique-ar', this.value);
});

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
// Fonctions vaccin Tissulaire avec sar
function Tissulaireavecsar(dateMorsure, poidsInput) {
    const { nom, prenom, dob } = patientInfo;
    const animal = "chien"; // Animal en cause (valeur par défaut)

    // Convertir le poids en nombre
    const poids = parseFloat(poidsInput);

    // Calcul du SAR selon les règles spécifiées
    let sar = poids / 5;
    if (poids >= 75) {
        sar = 15;
    }
    // Arrondir à  2 décimales
    sar = Math.round(sar * 100) / 100;

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

    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";
    const docteur = localStorage.getItem('docteur') || "";

    // Vérifier le format choisi
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';

    let enteteContent = '';
    if (avecEntete) {
        enteteContent = generateHeader();
    } else {
        // Espace vide pour garder la meme mise en page
        enteteContent = '<div style="height: 150px;"></div>';
    }


    const certificatContent = `
<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<title>Certificat Antirabique - Tissulaire sans SAR</title>
<style>
body {
font-family: Arial, sans-serif;
padding: 10px;
background-color: #f9f9f9;
}
.certificat {
background-color: white;
border: 1px solid #ddd;
padding: 20px;
max-width: 800px;
margin: 0 auto;
margin-top: 05px;
box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
}
h1 {
text-align: center;
color: #333;
text-decoration: underline;
font-size: 15px;
margin-top: 05px;
}
p {
line-height: 1.4;
color: #555;
}
.small-text {
        font-size: 12px;
    }
    .highlight {
        background-color: #f0f0f0;
        padding: 2px 5px;
        border-radius: 3px;
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
.highlight {
background-color: #f0f0f0;
padding: 2px 5px;
border-radius: 3px;
font-size: 9pt !important;
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
}
</style>
</head>
<body>
${enteteContent}

<div class="certificat">
<h1>Schéma Antirabique - Tissulaire avec SAR</h1>
<p>
Nom et Prenom : <strong><input type="text" value="${nom} ${prenom}" style="width: auto;"></strong><br>
Date de naissance : <strong><input type="text" value="${dob}" style="width: auto;"></strong><br>
Animal en cause : <strong><input type="text" value="${animal}" style="width: auto;"></strong><br>
Poids : <strong><input type="text" value="${poidsInput} kg" style="width: auto;"></strong><br>
        SAR calculé : <span class="highlight">${sar} cc</span> ${poids >= 75 ? "(valeur max à  15 cc)" : ""}<br>
        <span style="font-size: smaller;">Classe 03, schéma choisi : vaccin tissulaire / avec SAR</span><br>


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
<p style="text-align: right; margin-top: 15px;">
Medecin traitant <br>
<span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>&nbsp&nbsp&nbsp&nbsp&nbsp;
</p>
</div>

<div class="print-button">
<button id="printButton">Imprimer le schéma</button>
<button id="saveButtonRc2">sauvegarder</button>
</div>
<script src="print.js"></script>

<script>
document.addEventListener('DOMContentLoaded', function () {
document.getElementById('polyclinique').addEventListener('input', function () {
localStorage.setItem('polyclinique', this.value);
});

document.getElementById('polyclinique-ar').addEventListener('input', function () {
localStorage.setItem('polyclinique-ar', this.value);
});

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
    } else {
        console.log("Popup bloquée par le navigateur.");
    }
}


// Fonctions vaccin cellulaire type 03
function vaccinc3(dateMorsure, poidsInput) {
    const { nom, prenom, dob } = patientInfo;
    const poids = parseFloat(poidsInput);
    let sar = poids / 5;
    if (poids >= 75) {
        sar = 15;
    }
    sar = Math.round(sar * 100) / 100;

    const dateJour0 = new Date(dateMorsure);
    const datePlus7 = new Date(dateJour0);
    datePlus7.setDate(dateJour0.getDate() + 7);
    const datePlus21 = new Date(dateJour0);
    datePlus21.setDate(dateJour0.getDate() + 21);

    const dateFormattedJour0 = `${dateJour0.getFullYear()}-${String(dateJour0.getMonth() + 1).padStart(2, '0')}-${String(dateJour0.getDate()).padStart(2, '0')}`;
    const dateFormattedPlus7 = `${datePlus7.getFullYear()}-${String(datePlus7.getMonth() + 1).padStart(2, '0')}-${String(datePlus7.getDate()).padStart(2, '0')}`;
    const dateFormattedPlus21 = `${datePlus21.getFullYear()}-${String(datePlus21.getMonth() + 1).padStart(2, '0')}-${String(datePlus21.getDate()).padStart(2, '0')}`;

    const docteur = localStorage.getItem('docteur') || "";
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';
    let enteteContent = avecEntete ? generateHeader() : '<div style="height: 155px;"></div>';

    const zagreb3Window = window.open(chrome.runtime.getURL('zagreb3-certificat.html'), '_blank');

    zagreb3Window.onload = function () {
        zagreb3Window.certData = {
            nom: nom,
            prenom: prenom,
            dob: dob,
            docteur: docteur,
            poidsInput: poidsInput,
            sar: sar,
            dateFormattedJour0: dateFormattedJour0,
            dateFormattedPlus7: dateFormattedPlus7,
            dateFormattedPlus21: dateFormattedPlus21,
            enteteContent: enteteContent
        };

        zagreb3Window.dispatchEvent(new Event('certDataReady'));
    };
}

// Fonction pour le schéma tissulaire avec SAR (classe 3 T)
function vaccint3(dateMorsure, poidsInput) {
    const { nom, prenom, dob } = patientInfo;
    const poids = parseFloat(poidsInput);
    let sar = poids / 5;
    if (poids >= 75) {
        sar = 15;
    }
    sar = Math.round(sar * 100) / 100;

    // Calcul des dates pour le schéma tissulaire avec SAR
    const dateJour0 = new Date(dateMorsure);
    const datePlus1 = new Date(dateJour0);
    datePlus1.setDate(dateJour0.getDate() + 1);
    const datePlus2 = new Date(dateJour0);
    datePlus2.setDate(dateJour0.getDate() + 2);
    const datePlus3 = new Date(dateJour0);
    datePlus3.setDate(dateJour0.getDate() + 3);
    const datePlus4 = new Date(dateJour0);
    datePlus4.setDate(dateJour0.getDate() + 4);
    const datePlus5 = new Date(dateJour0);
    datePlus5.setDate(dateJour0.getDate() + 5);
    const datePlus6 = new Date(dateJour0);
    datePlus6.setDate(dateJour0.getDate() + 6);
    const datePlus10 = new Date(dateJour0);
    datePlus10.setDate(dateJour0.getDate() + 10);
    const datePlus14 = new Date(dateJour0);
    datePlus14.setDate(dateJour0.getDate() + 14);
    const datePlus29 = new Date(dateJour0);
    datePlus29.setDate(dateJour0.getDate() + 29);
    const datePlus90 = new Date(dateJour0);
    datePlus90.setDate(dateJour0.getDate() + 90);

    // Formatage des dates
    const formatDate = (date) => `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;

    const docteur = localStorage.getItem('docteur') || "";
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';
    let enteteContent = avecEntete ? generateHeader() : '<div style="height: 155px;"></div>';

    // Ouvrir la nouvelle page HTML
    const tissulaireAvecSARWindow = window.open(chrome.runtime.getURL('schema-tissulaire-avec-sar-certificat.html'), '_blank');

    tissulaireAvecSARWindow.onload = function () {
        tissulaireAvecSARWindow.certData = {
            nom: nom,
            prenom: prenom,
            dob: dob,
            docteur: docteur,
            poidsInput: poidsInput,
            sar: sar,
            dateFormattedJour0: formatDate(dateJour0),
            dateFormattedPlus1: formatDate(datePlus1),
            dateFormattedPlus2: formatDate(datePlus2),
            dateFormattedPlus3: formatDate(datePlus3),
            dateFormattedPlus4: formatDate(datePlus4),
            dateFormattedPlus5: formatDate(datePlus5),
            dateFormattedPlus6: formatDate(datePlus6),
            dateFormattedPlus10: formatDate(datePlus10),
            dateFormattedPlus14: formatDate(datePlus14),
            dateFormattedPlus29: formatDate(datePlus29),
            dateFormattedPlus90: formatDate(datePlus90),
            enteteContent: enteteContent
        };

        tissulaireAvecSARWindow.dispatchEvent(new Event('certDataReady'));
    };
}

// Nouvelles fonctions pour les certificats Classe 2
function prophylaxiePreExpositionSchema1Classe2(dateMorsure, poidsInput) {
    const { nom, prenom, dob } = patientInfo;
    const poids = parseFloat(poidsInput);
    const animal = "chien"; // Animal en cause (valeur par défaut)

    const dateJour0 = new Date(dateMorsure);
    const datePlus3 = new Date(dateJour0);
    datePlus3.setDate(dateJour0.getDate() + 3);

    // Formatage des dates
    const formatDate = (date) => `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;

    const docteur = localStorage.getItem('docteur') || "";
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';
    let enteteContent = avecEntete ? generateHeader() : '<div style="height: 155px;"></div>';

    // Ouvrir la nouvelle page HTML
    const schema1Window = window.open(chrome.runtime.getURL('prophylaxie-pre-exposition-schema1-c2-certificat.html'), '_blank');

    schema1Window.onload = function () {
        schema1Window.certData = {
            nom: nom,
            prenom: prenom,
            dob: dob,
            docteur: docteur,
            poidsInput: poidsInput,
            dateFormattedJour0: formatDate(dateJour0),
            dateFormattedPlus3: formatDate(datePlus3),
            enteteContent: enteteContent
        };

        schema1Window.dispatchEvent(new Event('certDataReady'));
    };
}

function prophylaxiePreExpositionSchema2Classe2(dateMorsure) {
    const { nom, prenom, dob } = patientInfo;
    const animal = "chien"; // Animal en cause (valeur par défaut)

    const dateJour0 = new Date(dateMorsure);

    // Formatage des dates
    const formatDate = (date) => `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;

    const docteur = localStorage.getItem('docteur') || "";
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';
    let enteteContent = avecEntete ? generateHeader() : '<div style="height: 155px;"></div>';

    // Ouvrir la nouvelle page HTML
    const schema2Window = window.open(chrome.runtime.getURL('prophylaxie-pre-exposition-schema2-c2-certificat.html'), '_blank');

    schema2Window.onload = function () {
        schema2Window.certData = {
            nom: nom,
            prenom: prenom,
            dob: dob,
            docteur: docteur,
            dateFormattedJour0: formatDate(dateJour0),
            enteteContent: enteteContent
        };

        schema2Window.dispatchEvent(new Event('certDataReady'));
    };
}

// Nouvelle fonction pour le certificat Risque Hémorragique Classe 2
function risqueHemorragiqueClasse2(dateMorsure, poidsInput) {
    const { nom, prenom, dob } = patientInfo;
    const poids = parseFloat(poidsInput);
    let sar = poids / 5;
    if (poids >= 75) {
        sar = 15;
    }
    sar = Math.round(sar * 100) / 100;

    // Calcul des dates pour le schéma risque hémorragique
    const dateJour0 = new Date(dateMorsure);
    const datePlus3 = new Date(dateJour0);
    datePlus3.setDate(dateJour0.getDate() + 3);
    const datePlus7 = new Date(dateJour0);
    datePlus7.setDate(dateJour0.getDate() + 7);

    // Formatage des dates
    const formatDate = (date) => `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;

    const docteur = localStorage.getItem('docteur') || "";
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';
    let enteteContent = avecEntete ? generateHeader() : '<div style="height: 155px;"></div>';

    // Ouvrir la nouvelle page HTML
    const risqueHemorragiqueWindow = window.open(chrome.runtime.getURL('schema-risque-hemorragique-c2-certificat.html'), '_blank');

    risqueHemorragiqueWindow.onload = function () {
        risqueHemorragiqueWindow.certData = {
            nom: nom,
            prenom: prenom,
            dob: dob,
            docteur: docteur,
            poidsInput: poidsInput,
            sar: sar,
            dateFormattedJour0: formatDate(dateJour0),
            dateFormattedPlus3: formatDate(datePlus3),
            dateFormattedPlus7: formatDate(datePlus7),
            enteteContent: enteteContent
        };

        risqueHemorragiqueWindow.dispatchEvent(new Event('certDataReady'));
    };
}

// Nouvelles fonctions pour les certificats Avec ATCD Vaccinaux Classe 3
function prophylaxiePreExpositionSchema1Classe3(dateMorsure, poidsInput) {
    const { nom, prenom, dob } = patientInfo;
    const poids = parseFloat(poidsInput);
    const animal = "chien"; // Animal en cause (valeur par défaut)

    const dateJour0 = new Date(dateMorsure);
    const datePlus3 = new Date(dateJour0);
    datePlus3.setDate(dateJour0.getDate() + 3);

    // Formatage des dates
    const formatDate = (date) => `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;

    const docteur = localStorage.getItem('docteur') || "";
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';
    let enteteContent = avecEntete ? generateHeader() : '<div style="height: 155px;"></div>';

    // Ouvrir la nouvelle page HTML
    const schema1Window = window.open(chrome.runtime.getURL('prophylaxie-pre-exposition-schema1-certificat.html'), '_blank');

    schema1Window.onload = function () {
        schema1Window.certData = {
            nom: nom,
            prenom: prenom,
            dob: dob,
            docteur: docteur,
            poidsInput: poidsInput,
            dateFormattedJour0: formatDate(dateJour0),
            dateFormattedPlus3: formatDate(datePlus3),
            enteteContent: enteteContent
        };

        schema1Window.dispatchEvent(new Event('certDataReady'));
    };
}

function prophylaxiePreExpositionSchema2Classe3(dateMorsure, poidsInput) {
    const { nom, prenom, dob } = patientInfo;
    const poids = parseFloat(poidsInput);
    const animal = "chien"; // Animal en cause (valeur par défaut)

    const dateJour0 = new Date(dateMorsure);

    // Formatage des dates
    const formatDate = (date) => `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;

    const docteur = localStorage.getItem('docteur') || "";
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';
    let enteteContent = avecEntete ? generateHeader() : '<div style="height: 155px;"></div>';

    // Ouvrir la nouvelle page HTML
    const schema2Window = window.open(chrome.runtime.getURL('prophylaxie-pre-exposition-schema2-certificat.html'), '_blank');

    schema2Window.onload = function () {
        schema2Window.certData = {
            nom: nom,
            prenom: prenom,
            dob: dob,
            docteur: docteur,
            poidsInput: poidsInput,
            dateFormattedJour0: formatDate(dateJour0),
            enteteContent: enteteContent
        };

        schema2Window.dispatchEvent(new Event('certDataReady'));
    };
}

// Nouvelle fonction pour le certificat Risque Hémorragique Classe 3
function risqueHemorragique3(dateMorsure, poidsInput) {
    const { nom, prenom, dob } = patientInfo;
    const poids = parseFloat(poidsInput);
    let sar = poids / 5;
    if (poids >= 75) {
        sar = 15;
    }
    sar = Math.round(sar * 100) / 100;

    // Calcul des dates pour le schéma risque hémorragique
    const dateJour0 = new Date(dateMorsure);
    const datePlus3 = new Date(dateJour0);
    datePlus3.setDate(dateJour0.getDate() + 3);
    const datePlus7 = new Date(dateJour0);
    datePlus7.setDate(dateJour0.getDate() + 7);

    // Formatage des dates
    const formatDate = (date) => `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;

    const docteur = localStorage.getItem('docteur') || "";
    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';
    let enteteContent = avecEntete ? generateHeader() : '<div style="height: 155px;"></div>';

    // Ouvrir la nouvelle page HTML
    const risqueHemorragiqueWindow = window.open(chrome.runtime.getURL('schema-risque-hemorragique-certificat.html'), '_blank');

    risqueHemorragiqueWindow.onload = function () {
        risqueHemorragiqueWindow.certData = {
            nom: nom,
            prenom: prenom,
            dob: dob,
            docteur: docteur,
            poidsInput: poidsInput,
            sar: sar,
            dateFormattedJour0: formatDate(dateJour0),
            dateFormattedPlus3: formatDate(datePlus3),
            dateFormattedPlus7: formatDate(datePlus7),
            enteteContent: enteteContent
        };

        risqueHemorragiqueWindow.dispatchEvent(new Event('certDataReady'));
    };
}

function risqueHemorragique(dateMorsure, poidsInput) {
    const { nom, prenom, dob } = patientInfo;
    const animal = "chien"; // Animal en cause (valeur par défaut)

    // Convertir le poids en nombre
    const poids = parseFloat(poidsInput);

    // Calcul du SAR selon les règles spécifiées
    let sar = poids / 5;
    if (poids >= 75) {
        sar = 15;
    }
    // Arrondir à  2 décimales
    sar = Math.round(sar * 100) / 100;

    const dateJour0 = new Date(dateMorsure);
    const datePlus3 = new Date(dateJour0);
    datePlus3.setDate(dateJour0.getDate() + 3);
    const datePlus7 = new Date(dateJour0);
    datePlus7.setDate(dateJour0.getDate() + 7);

    const dateFormattedJour0 = `${dateJour0.getFullYear()}-${String(dateJour0.getMonth() + 1).padStart(2, '0')}-${String(dateJour0.getDate()).padStart(2, '0')}`;
    const dateFormattedPlus3 = `${datePlus3.getFullYear()}-${String(datePlus3.getMonth() + 1).padStart(2, '0')}-${String(datePlus3.getDate()).padStart(2, '0')}`;
    const dateFormattedPlus7 = `${datePlus7.getFullYear()}-${String(datePlus7.getMonth() + 1).padStart(2, '0')}-${String(datePlus7.getDate()).padStart(2, '0')}`;

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
        enteteContent = '<div style="height: 155px;">\</div>';
    }

    const certificatContent = `
    <!DOCTYPE html>
    <html lang="fr">
    <head>
    <meta charset="UTF-8">
    <title>Certificat Antirabique - Risque Hémorragique</title>
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
    .highlight {
        background-color: #f0f0f0;
        padding: 2px 5px;
        border-radius: 3px;
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
        <h2>Selon l'instruction N16 du 15 Juillet 2024 relative à  la conduite à  tenir devant un risque rabique</h2>
        <p>
        NOM : <strong><input type="text" value="${nom} ${prenom}" style="width: auto;"></strong><br>
        Date de naissance : <strong><input type="text" value="${dob}" style="width: auto;"></strong><br>
		Animal en cause : <strong><input type="text" value="${animal}" style="width: auto;"></strong><br>
        Poids : <strong><input type="text" value="${poidsInput} kg" style="width: auto;"></strong><br>
        SAR calculé : <span class="highlight">${sar} cc</span> ${poids >= 75 ? "(valeur max à  15 cc)" : ""}<br><br>
		<br>
        <span class="small-text">Schéma adapté pour patient à  risque hémorragique ou quantités limitées</span>
		<br>
        </p>
        <p>
         <strong>Personne sans antécédent de vaccination:</strong><br>
         Un total de six doses : 2 doses en deux [2] sites différents<br><br>
         Jour 0 : <input type="date" id="dateJour0" value="${dateFormattedJour0}" readonly> ( 2 doses en ID + SAR )<br>
         Jour 3 : <input type="date" id="datePlus3" value="${dateFormattedPlus3}" readonly> ( 2 doses en ID )<br>
		 Jour 7: <input type="date" id="datePlus7" value="${dateFormattedPlus7}" readonly> ( 2 doses en ID )<br><br>

		NB: 01 dose =0.1ml
		<br>
        </p>
        <p style="text-align: right; margin-top: 30px;">
        Medecin traitant <br>
        <span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>&nbsp&nbsp&nbsp&nbsp&nbsp;
        </p>
    </div>

    <div class="print-button">
        <button id="printButton">Imprimer le schéma</button>
        <button id="saveButtonRc2">sauvegarder</button>
    </div>
    <script src="print.js"></script>
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

        // Add event listener after content loads
        newWindow.addEventListener('DOMContentLoaded', function () {
            const polycliniqueInput = newWindow.document.getElementById('polyclinique');
            if (polycliniqueInput) {
                polycliniqueInput.addEventListener('input', function () {
                    localStorage.setItem('polyclinique', this.value);
                });
            }

            const polycliniqueArInput = newWindow.document.getElementById('polyclinique-ar');
            if (polycliniqueArInput) {
                polycliniqueArInput.addEventListener('input', function () {
                    localStorage.setItem('polyclinique-ar', this.value);
                });
            }
        });

        // If DOM is already loaded, add listeners immediately
        setTimeout(() => {
            const polycliniqueInput = newWindow.document.getElementById('polyclinique');
            if (polycliniqueInput) {
                polycliniqueInput.addEventListener('input', function () {
                    localStorage.setItem('polyclinique', this.value);
                });
            }

            const polycliniqueArInput = newWindow.document.getElementById('polyclinique-ar');
            if (polycliniqueArInput) {
                polycliniqueArInput.addEventListener('input', function () {
                    localStorage.setItem('polyclinique-ar', this.value);
                });
            }
        }, 100);
    } else {
        console.log("Popup bloquée par le navigateur.");
    }
}

function essen3(dateMorsure, poidsInput) {
    console.log("Essen3 function called.");
    const { nom, prenom, dob } = patientInfo;
    const poids = parseFloat(poidsInput) || 0;

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

    const docteur = localStorage.getItem('docteur') || "";
    const sar = (poids > 75) ? 15 : (poids / 5);

    const avecEntete = localStorage.getItem('certificatFormat') === 'avecEntete';
    let enteteContent = avecEntete ? generateHeader() : '<div style="height: 155px;"></div>';

    console.log("Attempting to open new window for essen3-certificat.html");
    const essen3Window = window.open(chrome.runtime.getURL('essen3-certificat.html'), '_blank');

    if (!essen3Window || essen3Window.closed || typeof essen3Window.closed == 'undefined') {
        console.error("Popup window was blocked or failed to open.");
        alert("Le fenêtre popup a été bloquée par le navigateur. Veuillez autoriser les popups pour cette extension.");
        return;
    }
    console.log("Popup window opened successfully.");

    essen3Window.onload = function () {
        console.log("Essen3 popup window loaded.");
        essen3Window.certData = {
            nom: nom,
            prenom: prenom,
            dob: dob,
            docteur: docteur,
            poidsInput: poidsInput,
            sar: Math.round(sar * 100) / 100,
            dateFormattedJour0: dateFormattedJour0,
            dateFormattedPlus3: dateFormattedPlus3,
            dateFormattedPlus7: dateFormattedPlus7,
            dateFormattedPlus14: dateFormattedPlus14,
            enteteContent: enteteContent
        };

        essen3Window.dispatchEvent(new Event('certDataReady'));
        console.log("certDataReady event dispatched.");
    };
}

// Version sans poids pour risqueHemorragique
function risqueHemorragiquec2(dateMorsure) {
    const { nom, prenom, dob } = patientInfo;
    const animal = "chien"; // Animal en cause (valeur par défaut)

    const dateJour0 = new Date(dateMorsure);
    const datePlus3 = new Date(dateJour0);
    datePlus3.setDate(dateJour0.getDate() + 3);
    const datePlus7 = new Date(dateJour0);
    datePlus7.setDate(dateJour0.getDate() + 7);

    const dateFormattedJour0 = `${dateJour0.getFullYear()}-${String(dateJour0.getMonth() + 1).padStart(2, '0')}-${String(dateJour0.getDate()).padStart(2, '0')}`;
    const dateFormattedPlus3 = `${datePlus3.getFullYear()}-${String(datePlus3.getMonth() + 1).padStart(2, '0')}-${String(datePlus3.getDate()).padStart(2, '0')}`;
    const dateFormattedPlus7 = `${datePlus7.getFullYear()}-${String(datePlus7.getMonth() + 1).padStart(2, '0')}-${String(datePlus7.getDate()).padStart(2, '0')}`;

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
        enteteContent = '<div style="height: 155px;">\</div>';
    }

    const certificatContent = `
    <!DOCTYPE html>
    <html lang="fr">
    <head>
    <meta charset="UTF-8">
    <title>Certificat Antirabique - Risque Hémorragique</title>
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
    .highlight {
        background-color: #f0f0f0;
        padding: 2px 5px;
        border-radius: 3px;
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
        <h2>Selon l'instruction N16 du 15 Juillet 2024 relative à  la conduite à  tenir devant un risque rabique</h2>
        <p>
        NOM : <strong><input type="text" value="${nom} ${prenom}" style="width: auto;"></strong><br>
        Date de naissance : <strong><input type="text" value="${dob}" style="width: auto;"></strong><br>
		Animal en cause : <strong><input type="text" value="${animal}" style="width: auto;"></strong><br>
		<br><br>
        <span class="small-text">Schéma adapté pour patient à  risque hémorragique ou quantités limitées</span>
		<br><br>
        </p>
        <p>
         <strong>Personne sans antécédent de vaccination:</strong><br><br>
         Un total de six doses : 2 doses en deux [2] sites différents<br><br>
         Jour 0 : <input type="date" id="dateJour0" value="${dateFormattedJour0}" readonly> ( 2 doses en ID  )<br>
         Jour 3 : <input type="date" id="datePlus3" value="${dateFormattedPlus3}" readonly> ( 2 doses en ID )<br>
		 Jour 7: <input type="date" id="datePlus7" value="${dateFormattedPlus7}" readonly> ( 2 doses en ID )<br><br>

		NB: 01 dose =0.1ml
        </p>
        <p style="text-align: right; margin-top: 30px;">
        Medecin traitant <br>
        <span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>&nbsp&nbsp&nbsp&nbsp&nbsp;
        </p>
    </div>

    <div class="print-button">
        <button id="printButton">Imprimer le schéma</button>
        <button id="saveButtonRc2">sauvegarder</button>
    </div>
    <script src="print.js"></script>
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

        // Add event listener after content loads
        newWindow.addEventListener('DOMContentLoaded', function () {
            const polycliniqueInput = newWindow.document.getElementById('polyclinique');
            if (polycliniqueInput) {
                polycliniqueInput.addEventListener('input', function () {
                    localStorage.setItem('polyclinique', this.value);
                });
            }

            const polycliniqueArInput = newWindow.document.getElementById('polyclinique-ar');
            if (polycliniqueArInput) {
                polycliniqueArInput.addEventListener('input', function () {
                    localStorage.setItem('polyclinique-ar', this.value);
                });
            }
        });

        // If DOM is already loaded, add listeners immediately
        setTimeout(() => {
            const polycliniqueInput = newWindow.document.getElementById('polyclinique');
            if (polycliniqueInput) {
                polycliniqueInput.addEventListener('input', function () {
                    localStorage.setItem('polyclinique', this.value);
                });
            }

            const polycliniqueArInput = newWindow.document.getElementById('polyclinique-ar');
            if (polycliniqueArInput) {
                polycliniqueArInput.addEventListener('input', function () {
                    localStorage.setItem('polyclinique-ar', this.value);
                });
            }
        }, 100);
    } else {
        console.log("Popup bloquée par le navigateur.");
    }
}








function essen3(dateMorsure, poidsInput) {

    const { nom, prenom, dob } = patientInfo;
    const animal = "chien"; // Animal en cause (valeur par défaut)

    // Convertir le poids en nombre
    const poids = parseFloat(poidsInput);

    // Calcul du SAR selon les règles spécifiées
    let sar = poids / 5;
    if (poids >= 75) {
        sar = 15;
    }
    // Arrondir à  2 décimales
    sar = Math.round(sar * 100) / 100;

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
    <title>Certificat Antirabique - Essen</title>
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
        font-size: 8px;
        margin-top: 5px;
        margin-bottom: 15px;
        text-decoration: underline;
        font-size: 10px;
    }
    p {
        line-height: 1.5;
        color: #555;
    }
    .small-text {
        font-size: 12px;
    }
    .highlight {
        background-color: #f0f0f0;
        padding: 2px 5px;
        border-radius: 3px;
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
        h2 {
            font-size: 8pt !important;
            margin: 3px 0 !important;
        }
        p {
            font-size: 9pt !important;
            margin: 2px 0 !important;
            line-height: 1.2 !important;
        }
        .small-text {
            font-size: 8pt !important;
        }
        .highlight {
            background-color: #f0f0f0;
            padding: 2px 5px;
            border-radius: 3px;
            font-size: 9pt !important;
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
    }
    </style>
    </head>
    <body>
    ${enteteContent}

    <div class="certificat">
        <h1>Schéma Antirabique - Essen3</h1>
        <h2>Selon l'instruction N16 du 15 Juillet 2024 relative à  la conduite à  tenir devant un risque rabique</h2>
        <p>
        NOM : <strong><input type="text" value="${nom} ${prenom}" style="width: auto;"></strong><br>
        Date de naissance : <strong><input type="text" value="${dob}" style="width: auto;"></strong><br>
		Animal en cause : <strong><input type="text" value="${animal}" style="width: auto;"></strong><br>
        Poids : <strong><input type="text" value="${poidsInput} kg" style="width: auto;"></strong><br>
        SAR calculé : <span class="highlight">${sar} cc</span> ${poids >= 75 ? "(valeur max à  15 cc)" : ""}<br>
        <span class="small-text">Classe 03, schéma choisi : vaccin cellulaire / schéma de Essen / avec SAR</span>
        </p>
        <p>
        Jour 0 : <input type="date" id="dateJour0" value="${dateFormattedJour0}" readonly> ( dans le deltoïde )<br>
        Jour 3 : <input type="date" id="datePlus3" value="${dateFormattedPlus3}" readonly><br>
        Jour 7 : <input type="date" id="datePlus7" value="${dateFormattedPlus7}" readonly><br>
        Jour 14 : <input type="date" id="datePlus14" value="${dateFormattedPlus14}" readonly><br>
        <br><br>
		Pour les immunodéprimées si sérologie de contrôle après 02 a 04 semaines de vaccin < 0.5 ui/ml (ou s’il n'est pas faisable) ajouter une autre dose de vaccin.
		<br><br>
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
        <button id="saveButtonRc2">sauvegarder</button>
    </div>
    <script src="print.js"></script>

    <script>
    document.addEventListener('DOMContentLoaded', function () {
        document.getElementById('polyclinique').addEventListener('input', function () {
            localStorage.setItem('polyclinique', this.value);
        });

        document.getElementById('polyclinique-ar').addEventListener('input', function () {
            localStorage.setItem('polyclinique-ar', this.value);
        });

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
    } else {
        console.log("Popup bloquée par le navigateur.");
    }
}











//===========================================



//requisitionApte()
function requisitionApte() {
    // Fermer la modale si elle existe
    const existingModal = document.querySelector('.modal');
    if (existingModal) {
        existingModal.remove();
        window.location.reload();
    }

    const { nom, prenom, dob } = patientInfo;

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
      <strong><input type="text" value="${nom} ${prenom}" readonly placeholder="Nom et prénom" style="width: 180px; padding: 4px; border: 1px solid #ddd; border-radius: 4px; margin: 0 5px;"></strong>
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


// Fonction pour le certificat d'inaptitude au sport
function ouvrirCertificatInaptitudeSport() {
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
    <strong><input type="text" value="${nom} ${prenom}" style="width: 180px;"></strong>
    <br>
    née le : <strong><input type="text" value="${dob}" style="width: 120px;"></strong>
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

// Fonction pour générer le certificat de bonne santé
function ouvrirCertificatBonSante() {
    // Ouvrir le template cert-bon-sante-template.html dans un nouvel onglet
    chrome.tabs.create({ url: chrome.runtime.getURL('cert-bon-sante-template.html') });
}

function ouvrirCertificatMalVision() {
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







// Fonction pour générer le certificat de prophylaxie pré-exposition pour patients immunocompétents
function genererCertificatProphylaxieImmunocompetent(dateDebut) {
    const { nom, prenom, dob } = patientInfo;

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
    <title>Prophylaxie pré-exposition - Patient Immunocompétent</title>
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
        <h1>Prophylaxie pré-exposition (Immunocompétent)</h1>
        <h2>Selon l'instruction N16 du 15 Juillet 2024 relative à la conduite à tenir devant un risque rabique</h2>
        <p>
        NOM : <strong><input type="text" value="${nom} ${prenom}" style="width: auto;"></strong><br>
        Date de naissance : <strong><input type="text" value="${dob}" style="width: auto;"></strong><br>
        </p>
        <p>
         <strong style="font-size: 14px;">Patient Immunocompétent - Prophylaxie pré-exposition :</strong><br>
         Un total de trois (3) doses :<br><br>
         Jour 0 : <input type="date" id="dateJour0" value="${dateFormattedJour0}" readonly> (dans le deltoïde)<br>
         Jour 7 : <input type="date" id="datePlus7" value="${dateFormattedPlus7}" readonly> <br>
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
    </div>
    <script src="print.js"></script>

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
    const { nom, prenom, dob } = patientInfo;

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
        Date de naissance : <strong><input type="text" value="${dob}" style="width: auto;"></strong><br>
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
    </div>
    <script src="print.js"></script>

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

// Fonction pour générer le certificat Avec ATCD Vaccinaux (IM)
function avecATCDVaccinauxIM(dateMorsure, poidsInput) {
    const { nom, prenom, dob } = patientInfo;
    const poids = parseFloat(poidsInput);

    const dateJour0 = new Date(dateMorsure);
    const datePlus3 = new Date(dateJour0);
    datePlus3.setDate(dateJour0.getDate() + 3);

    const dateFormattedJour0 = `${dateJour0.getFullYear()}-${String(dateJour0.getMonth() + 1).padStart(2, '0')}-${String(dateJour0.getDate()).padStart(2, '0')}`;
    const dateFormattedPlus3 = `${datePlus3.getFullYear()}-${String(datePlus3.getMonth() + 1).padStart(2, '0')}-${String(datePlus3.getDate()).padStart(2, '0')}`;

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

    const animal = "chien"; // Animal en cause (valeur par défaut)

    const certificatContent = `
    <!DOCTYPE html>
    <html lang="fr">
    <head>
    <meta charset="UTF-8">
    <title>Certificat Antirabique - Avec ATCD Vaccinaux (IM)</title>
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
        <h1>Schéma Antirabique - Avec ATCD Vaccinaux (IM)</h1>
        <h2>Selon l'instruction N16 du 15 Juillet 2024 relative à la conduite à tenir devant un risque rabique</h2>
        <p>
        NOM : <strong><input type="text" value="${nom} ${prenom}" style="width: auto;"></strong><br>
        Date de naissance : <strong><input type="text" value="${dob}" style="width: auto;"></strong><br>
		Animal en cause : <strong><input type="text" value="${animal}" style="width: auto;"></strong><br>
        <br><br>
        </p>
        <p>
         <strong style="font-size: 14px;">Personne ayant reçu une vaccination antirabique  (>= 02 doses):</strong><br>
         Schéma IM : Un total de deux (2) doses<br><br>
         Jour 0 : <input type="date" id="dateJour0" value="${dateFormattedJour0}" readonly> (dans le deltoïde)<br>
         Jour 3 : <input type="date" id="datePlus3" value="${dateFormattedPlus3}" readonly> (dans le deltoïde)<br>
         <br><br>
         NB: 01 dose = 1ml (voie intramusculaire)
        </p>
        <p style="text-align: right; margin-top: 30px;">
        Medecin traitant <br>
        <span class="docteur" style="font-weight: bold;">Dr ${docteur}</span>&nbsp&nbsp&nbsp&nbsp&nbsp;
        </p>
    </div>

    <div class="print-button">
        <button id="printButton">Imprimer le schéma</button>
        <button id="saveButtonRc2">sauvegarder</button>
    </div>
    <script src="print.js"></script>

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

// Fonction pour demander la date pour ATCD Prophylaxie Pré-exposition
function demanderDateEtPoidsATCDProphylaxie() {
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

// Add event listener for data retrieval button
document.addEventListener('DOMContentLoaded', function () {
    const dataRetrievalButton = document.getElementById('dataRetrievalButton');
    if (dataRetrievalButton) {
        dataRetrievalButton.addEventListener('click', function () {
            // Open the data retrieval page
            chrome.tabs.create({ url: chrome.runtime.getURL('recuperer-donnees.html') });
        });
    }
});
