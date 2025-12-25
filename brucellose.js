// Fonction pour capitaliser automatiquement les noms et prénoms
function capitalizeNames(text) {
    if (!text) return text;
    return text.toLowerCase().replace(/\b\w/g, letter => letter.toUpperCase());
}

// Fonction personnalisée pour conserver les retours à la ligne
function trimPreserveNewlines(text) {
    return text.replace(/^\s+|\s+$/g, '');
}

// Fonction pour sauvegarder les informations de la polyclinique et du docteur
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

// Fonction pour charger les données du patient
function loadPatientData() {
    // Récupérer les données du patient depuis localStorage avec capitalisation
    const patientInfo = {
        nom: capitalizeNames(localStorage.getItem("nom") || ""),
        prenom: capitalizeNames(localStorage.getItem("prenom") || ""),
        dob: localStorage.getItem("dateNaissance") || ""
    };
    localStorage.setItem('patientInfo', JSON.stringify(patientInfo));
}

// Fonction pour extraire les informations de la page
function extractInfo() {
    return {
        nom: capitalizeNames(localStorage.getItem("nom") || ""),
        prenom: capitalizeNames(localStorage.getItem("prenom") || ""),
        dob: localStorage.getItem("dateNaissance") || ""
    };
}

// Fonction pour charger les données de la polyclinique et du docteur
function loadData() {
    const polyclinique = localStorage.getItem('polyclinique') || "";
    const polycliniqueAr = localStorage.getItem('polyclinique-ar') || "";
    const docteur = localStorage.getItem('docteur') || "";

    // Vérifier l'existence des éléments avant de les modifier
    const polycliniqueInput = document.getElementById('polyclinique');
    const polycliniqueArInput = document.getElementById('polyclinique-ar');
    const docteurInput = document.getElementById('docteur');

    if (polycliniqueInput) polycliniqueInput.value = polyclinique;
    if (polycliniqueArInput) polycliniqueArInput.value = polycliniqueAr;
    if (docteurInput) docteurInput.value = docteur;
}

// Fonction pour stocker l'ID de l'onglet actif
function storeActiveTabId() {
    chrome.tabs.query({ active: true, currentWindow: true }, (tabs) => {
        if (tabs[0]) {
            localStorage.setItem('targetTabId', tabs[0].id);
        }
    });
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
                        <textarea readonly rows="2" style="text-align: center; width: 100%; border: none; background: none; resize: none; outline: none;">REPUBLIQUE ALGERIENNE DEMOCRATIQUE ET POPULAIRE</textarea>
                    </td>
                </tr>
                <tr>
                    <td colspan="4">
                        <textarea readonly rows="1" style="text-align: center; width: 100%; border: none; background: none; resize: none; outline: none;">MINISTERE DE LA SANTE</textarea>
                    </td>
                </tr>
                <tr>
                    <td colspan="2">
                        <textarea id="polyclinique" rows="2" style="width: 100%; border: none; background: none; font-size: 12px; resize: none; outline: none;">${polyclinique}</textarea>
                    </td>
                    <td colspan="2" style="text-align: right;">
                        <textarea id="polyclinique-ar" rows="2" style="text-align: right; width: 100%; border: none; background: none; font-size: 12px; resize: none; outline: none;" class="arabic-text">${polycliniqueAr}</textarea>
                    </td>
                </tr>
            </tbody>
        </table>
    </div>
    `;
}

// Fonction pour afficher le numéro depuis localStorage
function displayNumero() {
    const numero = localStorage.getItem('numero') || 'Aucun numéro trouvé';
   
}

// Fonction pour configurer les écouteurs de boutons
function setupButtonListeners() {
    const button = document.getElementById('createOrdonnance');
    if (button) {
        button.addEventListener('click', brucellose);
    } else {
        console.error('Bouton createOrdonnance non trouvé');
    }
}

// Initialiser l'extension au chargement de la page
document.addEventListener('DOMContentLoaded', () => {
    loadData();
    setupButtonListeners();
    loadPatientData();
    displayNumero();  // Affiche le numéro au chargement de la page
});

// Fonction principale : collecte les données du formulaire
function brucellose() {
    // Vérifier si le formulaire existe
    const form = document.getElementById('brucelloseForm');
    if (!form) {
        console.error('Le formulaire n\'a pas été trouvé');
        return;
    }
    
    // Afficher le numéro avant de générer l'ordonnance
displayNumero();

    // Récupérer les éléments du formulaire
    const radios = document.querySelectorAll('input[name="brucellose"]:checked');
    const selectedValue = radios.length > 0 ? radios[0].value : 'Aucune sélection';
    const poids = parseFloat(document.getElementById('poid')?.value) || 0;
    const patientInfo = JSON.parse(localStorage.getItem('patientInfo') || '{}');
    const nom = patientInfo.nom || '';
    const prenom = patientInfo.prenom || '';
    const dob = patientInfo.dob || '';
    const today = new Date();
    const day = String(today.getDate()).padStart(2, '0');
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const year = today.getFullYear();
    const formattedDate = `${day}/${month}/${year}`;

    // Calculate age
    let ageText = '';
    if (dob) {
        const birthDate = new Date(dob);
        const today = new Date();
        let age = today.getFullYear() - birthDate.getFullYear();
        const monthDiff = today.getMonth() - birthDate.getMonth();
        if (monthDiff < 0 || (monthDiff === 0 && today.getDate() < birthDate.getDate())) {
            age--;
        }
        ageText = ` (${age} ans)`;
    }

    let medicaments = [];
    switch (selectedValue) {
        case 'B aigue 01 er intention':
            medicaments = [
                { medicament: "Doxycycline 100 mg", posologie: "02 cp en une prise unique au milieu du repas", quantite: "42 jours" },
                { medicament: "Gentamicine 80mg", posologie: "une inj 02 fois par jour", quantite: "14 jours" }
            ];
            break;

        case 'B aigue 02 intention':
            medicaments = [
                { medicament: "Doxycycline 100 mg", posologie: "02 cp en une prise unique au milieu du repas", quantite: "42 jours" },
                { medicament: "Rifampicine 300mg ou (150)mg", posologie: "900mg en une prise à distance des repas", quantite: "42 jours" }
            ];
            break;

        case 'B aigue 03 intention A':
            medicaments = [
                { medicament: "Doxycycline 100 mg", posologie: "02 cp en une prise unique au milieu du repas", quantite: "60 jours" },
                { medicament: "Bacrim (cotrimoxazole) 480mg", posologie: "02cp 02 fois par jour", quantite: "60 jours" }
            ];
            break;

        case 'B aigue 03 intention B':
            medicaments = [
                { medicament: "Rifampicine 300mg ou (150)mg", posologie: "900mg en une prise à distance des repas", quantite: "42 jours" },
                { medicament: "Ciprofloxacine 500mg", posologie: "01 cp 02 fois par jour", quantite: "42 jours" }
            ];
            break;
			 case 'B chronique':
            alert("Pas d'indication au traitement sauf en cas d'existence d'un foyer focalisé");
            return;

        case 'femme enceinte':
            medicaments = [
			    { medicament: "Bacrim (cotrimoxazole) 480mg", posologie: "02cp 02 fois par jour", quantite: "42 jours" },
                { medicament: "Rifampicine 300mg ou (150)mg", posologie: "900mg en une prise à distance des repas", quantite: "42 jours" }    
            ];
            break;
        case 'enfant inf de 08 ans 01':
            if (poids === 0) {
                alert("Saisissez le poids de l'enfant");
                return;
            }
            
            // Calcul des posologies
            const posologieBacrim = ((poids * 75) / 200).toString() + " ml 02 fois par jour";
            
            // Définir le médicament et la posologie selon le poids
            let gentamicineMedicament;
            let posologieGentamicine;
            
            if (poids < 8) {
                gentamicineMedicament = "Gentamicine 40 mg";
                posologieGentamicine = "1/2 amp en IM";
            } else if (poids >= 8 && poids < 16) {
                gentamicineMedicament = "Gentamicine 40 mg";
                posologieGentamicine = "01 amp en IM";
            } else if (poids >= 16 && poids < 35) {
                gentamicineMedicament = "Gentamicine 80 mg";
                posologieGentamicine = "01 amp en IM";
            } else {
                gentamicineMedicament = "Gentamicine 80 mg";
                posologieGentamicine = "01 amp en IM 02 fois / jour";
            }
            
            medicaments = [
                { medicament: "Bacrim (cotrimoxazole) 240mg sirop", posologie: posologieBacrim, quantite: "42 jours" },
                { medicament: gentamicineMedicament, posologie: posologieGentamicine, quantite: "07 jours" }
            ];
            break;

        case 'enfant inf de 08 ans 02':
            if (poids === 0) {
                alert("Saisissez le poids de l'enfant");
                return;
            }
            
            // Calcul des posologies
            const posologieBactrim = ((poids * 75) / 200).toString() + " ml 02 fois par jour";
            const posologieRifampicine = (poids * 15).toString() + " mg en une prise à distance des repas";
            
            medicaments = [
                { medicament: "Bactrim (cotrimoxazole) 240mg sirop", posologie: posologieBactrim, quantite: "42 jours" },
                { medicament: "Rifampicine pédiatrique", posologie: posologieRifampicine, quantite: "42 jours" }
            ];
            break;
        case 'enfant plus de 08 ans 01':
            if (poids === 0) {
                alert("Saisissez le poids de l'enfant");
                return;
            }
            
            // Calcul des posologies
            const posologieDoxycycline = (poids * 5).toString() + " mg en une prise au milieu de repas";
            let medicament2;
            let posologie2;
            
            // Conditions pour Gentamicine selon le poids
            if (poids < 20) {
                medicament2 = "Gentamicine 40 mg";
                posologie2 = "1/2 amp en IM";
            } else if (poids >= 20 && poids < 40) {
                medicament2 = "Gentamicine 40 mg";
                posologie2 = "01 amp en IM";
            } else if (poids >= 40) {
                medicament2 = "Gentamicine 80 mg";
                posologie2 = "01 amp en IM";
            }
            
            medicaments = [
                { medicament: "Doxycycline 100 mg cp", posologie: posologieDoxycycline, quantite: "42 jours" },
                { medicament: medicament2, posologie: posologie2, quantite: "42 jours" }
            ];    
            break;

        case 'enfant plus de 08 ans 02':
            if (poids === 0) {
                alert("Saisissez le poids de l'enfant");
                return;
            }
            
            const posologieDoxi = (poids * 5).toString() + " mg en une prise au milieu de repas";
            const posologieRifam = (poids * 25).toString() + " mg en une prise à distance des repas";
            
            medicaments = [
                { medicament: "Doxycycline 100 mg cp", posologie: posologieDoxi, quantite: "42 jours" },
                { medicament: "Rifampicine pédiatrique", posologie: posologieRifam, quantite: "42 jours" }
            ];
			
    }

    if (medicaments.length === 0) {
        alert("Veuillez sélectionner un type de traitement");
        return;
    }

    const avecEntete = document.querySelector('input[name="header-option"][value="with-header"]').checked;
    let enteteContent = avecEntete ? generateHeader() : '<div style="height: 155px;"></div>';

    let itemsContent = '';
    medicaments.forEach((item, index) => {
        const nbr = index + 1;
        itemsContent += `
            <div style="margin-bottom: 15px;">
                <div style="display: flex; justify-content: space-between; align-items: baseline;">
                    <span style="flex: 1;">
                        <span style="font-weight: bold; min-width: 20px; display: inline-block;">${nbr}.</span>
                        <span style="white-space: normal;">${item.medicament}</span>
                    </span>
                    <span style="margin-left: 20px; white-space: nowrap;">${item.quantite}</span>
                </div>
                <div style="margin-left: 30px; color: #555; font-style: italic; margin-top: 5px;">${item.posologie}</div>
            </div>`;
    });

    const ordonnanceContent = `
    <!DOCTYPE html>
    <html lang="fr">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Ordonnance </title>
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
                position: relative;
            }
            .info {
                display: block;
                margin: 0px 6px 0px 0;
                font-size: 12px;
                line-height: 1.1;
            }
            h1 {
                text-align: center;
                color: #333;
                text-decoration: underline;
                font-size: 20px;
                margin-top: 15px;
                margin-bottom: 20px;
            }
            .patient-info {
                margin-top: 8px;
                margin-bottom: 12px;
                padding: 6px 8px;
                border: 1px solid #d0d0d0;
                border-radius: 3px;
                background-color: #f8f8f8;
                line-height: 1.1;
                width: 100%;
                box-sizing: border-box;
            }
            .info-row {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 3px;
                flex-wrap: nowrap;
                gap: 10px;
            }
            .info-row .info {
                margin: 0;
                flex: 2;
                white-space: nowrap;
            }
            .info-row .info.barcode {
                display: none;
            }
            @media screen {
                .patient-info {
                    min-height: 70px;
                    overflow: visible;
                }
                .info.barcode {
                    display: none !important;
                }
            }
            .medication-list {
                margin-top: 15px;
                margin-bottom: 20px;
                padding: 12px;
                border: 1px solid #d0d0d0;
                border-radius: 4px;
                background-color: #ffffff;
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
                    position: relative;
                }
                h1 {
                    font-size: 14pt !important;
                    margin: 5px 0 !important;
                }
                .patient-info {
                    position: relative !important;
                    margin: 0 !important;
                    padding: 0 !important;
                    border: none !important;
                    background: none !important;
                    max-height: none !important;
                    overflow: visible !important;
                    height: 50px !important;
                }
                .info {
                    font-size: 9pt !important;
                    position: absolute !important;
                    display: block !important;
                    margin: 0 !important;
                    color: #000 !important;
                }
                .info.nom { top: 60px !important; left: 20px !important; }
                .info.prenom { top: 75px !important; left: 20px !important; }
                .info.date-naissance { top: 90px !important; left: 20px !important; }
                .info.today { top: 75px !important; left: 270px !important; }
                .info.numero { top: 60px !important; left: 270px !important; }
                .info.barcode { top: 55px !important; left: 420px !important; }
                .medication-list {
                    margin-top: 100px !important;
                    margin-bottom: 10px !important;
                }
                .info.barcode {
                    display: block !important;
                }
                .info.barcode svg {
                    height: 30px !important;
                    width: 150px !important;
                }
                div[style*="margin-bottom: 15px"] {
                    margin-bottom: 5px !important;
                }
                span[style*="font-size: 14px"] {
                    font-size: 10px !important;
                }
                .print-button {
                    display: none;
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
            <h1>Ordonnance</h1>
            
            <div class="patient-info">
                <div class="info-row">
                    <div class="info nom"><strong>Nom :</strong> ${nom}</div>
                    <div class="info numero"><strong>Numéro :</strong> ${localStorage.getItem('numero') || ''}</div>
                </div>
                <div class="info-row">
                    <div class="info prenom"><strong>Prénom :</strong> ${prenom}</div>
                    <div class="info today"><strong>La date :</strong> ${formattedDate}</div>
                </div>
                <div class="info-row">
                    <div class="info date-naissance"><strong>Date de naissance :</strong> ${dob || 'Non renseigné'}${ageText}</div>
                    <div class="info barcode">
                        <svg id="barcode" data-numero="${localStorage.getItem('numero') || ''}"></svg>
                    </div>
                </div>
            </div>
            
            <div class="medication-list">
                ${itemsContent}
            </div>
        </div>
        <div class="print-button">
            <button id="printButton">Imprimer l'ordonnance</button>
        </div>
        <script src="JsBarcode.all.min.js"></script>
        <script src="barcode.js"></script>
        <script src="print.js"></script>
    </body>
    </html>`;



    const newWindow = window.open("", "_blank");
    if (newWindow) {
        newWindow.document.write(ordonnanceContent);
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
        alert("Le popup a été bloqué par votre navigateur. Veuillez autoriser les popups pour ce site.");
    }
}

// Affichage temporaire du format sélectionné
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


