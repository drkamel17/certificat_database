let ordonnancesTypesChargees = {};

window.mettreAJourListeOrdonnancesTypes = async function(ordonnancesTypes = null) {
    const select = document.getElementById('liste-ordonnances-types');
    
    if (!ordonnancesTypes) {
        ordonnancesTypes = JSON.parse(localStorage.getItem('ordonnancesTypesPourOrd') || '{}');
    }
    
    select.innerHTML = '<option value="">Sélectionnez une ordonnance type</option>';
    
    const nomsTries = Object.keys(ordonnancesTypes).sort((a, b) => a.localeCompare(b, 'fr', { sensitivity: 'base' }));
    
    const sourcesData = JSON.parse(localStorage.getItem('ordonnancesTypesSources') || '{}');
    
    nomsTries.forEach(nom => {
        const option = document.createElement('option');
        option.value = nom;
        option.textContent = nom;
        
        if (sourcesData[nom] === 'localStorage') {
            option.style.color = '#2563eb';
            option.style.fontWeight = '500';
        } else if (sourcesData[nom] === 'file') {
            option.style.color = '#059669';
            option.style.fontWeight = '500';
        }
        
        select.appendChild(option);
    });
};

document.addEventListener('DOMContentLoaded', async function() {
    // Charger les ordonnances types plus tard, après la définition de toutes les fonctions
    setTimeout(chargerOrdonnancesTypesDuFichier, 100);
    const leftColumn = document.getElementById('leftColumn');
    const rightColumn = document.getElementById('rightColumn');
    const resizeDivider = document.getElementById('resizeDivider');
    const toggleButton = document.getElementById('toggleButton');
    const returnButton = document.getElementById('returnButton');

    const ordonnanceList = [];

    function waitForMedicaments() {
        return new Promise((resolve) => {
            if (window.medicamentsLoaded) {
                resolve();
            } else {
                const checkInterval = setInterval(() => {
                    if (window.medicamentsLoaded) {
                        clearInterval(checkInterval);
                        resolve();
                    }
                }, 100);
            }
        });
    }

    await waitForMedicaments();

    if (toggleButton) {
        toggleButton.addEventListener('click', function() {
            leftColumn.classList.toggle('collapsed');
            rightColumn.classList.toggle('expanded');
            if (leftColumn.classList.contains('collapsed')) {
                toggleButton.innerHTML = '<i class="fas fa-compress-arrows-alt"></i> Mode normal';
            } else {
                toggleButton.innerHTML = '<i class="fas fa-expand-arrows-alt"></i> Mode plein écran';
            }
        });
    }

    if (returnButton) {
        returnButton.addEventListener('click', function() {
            leftColumn.classList.remove('collapsed');
            rightColumn.classList.remove('expanded');
        });
    }

    if (resizeDivider) {
        let isResizing = false;

        resizeDivider.addEventListener('mousedown', function(e) {
            isResizing = true;
            resizeDivider.classList.add('dragging');
        });

        document.addEventListener('mousemove', function(e) {
            if (!isResizing) return;
            const containerWidth = document.querySelector('.main-container').offsetWidth;
            const newLeftWidth = e.clientX;
            
            if (newLeftWidth > 300 && newLeftWidth < containerWidth - 400) {
                leftColumn.style.flex = '0 0 ' + newLeftWidth + 'px';
            }
        });

        document.addEventListener('mouseup', function() {
            isResizing = false;
            resizeDivider.classList.remove('dragging');
        });
    }

    const etablissemntBtn = document.getElementById('etablissemnt');
    if (etablissemntBtn) {
        etablissemntBtn.addEventListener('click', function() {
            alert('Fonctionnalité à implémenter: Insérer depuis DEM');
        });
    }

    const savePolycliniqueDocteurBtn = document.getElementById('savePolycliniqueDocteur');
    if (savePolycliniqueDocteurBtn) {
        savePolycliniqueDocteurBtn.addEventListener('click', function() {
            const polyclinique = document.getElementById('polyclinique').value;
            const polycliniqueAr = document.getElementById('polyclinique-ar').value;
            const docteur = document.getElementById('docteur').value;

            localStorage.setItem('polyclinique', polyclinique);
            localStorage.setItem('polyclinique-ar', polycliniqueAr);
            localStorage.setItem('docteur', docteur);

            afficherInfosEtablissement();
            alert('Informations sauvegardées !');
        });
    }

    const ajouterPersonnelBtn = document.getElementById('ajouter-personnel');
    if (ajouterPersonnelBtn) {
        ajouterPersonnelBtn.addEventListener('click', async function() {
            const medicament = prompt('Nom du médicament personnalisé:');
            if (medicament) {
                const meds = await chargerMedicaments();
                if (!meds.includes(medicament.trim())) {
                    sauvegarderMedicamentPersonnalise(medicament.trim());
                }
                await ajouterMedicamentALaListe(medicament.trim());
            }
        });
    }

    const ouvrirOptionsBtn = document.getElementById('ouvrir-options');
    if (ouvrirOptionsBtn) {
        ouvrirOptionsBtn.addEventListener('click', function() {
            window.open('options.html', '_blank');
        });
    }

    const resetListeBtn = document.getElementById('reset-liste');
    if (resetListeBtn) {
        resetListeBtn.addEventListener('click', async function() {
            if (confirm('Voulez-vous vraiment réinitialiser la liste ?')) {
                ordonnanceList.length = 0;
                await mettreAJourTableau();
            }
        });
    }

    const certificatBtn = document.getElementById('certificat');
    if (certificatBtn) {
        certificatBtn.addEventListener('click', function() {
            genererCertificat(false);
        });
    }

    const certdeceBtn = document.getElementById('certdece');
    if (certdeceBtn) {
        certdeceBtn.addEventListener('click', function() {
            genererCertificat(true);
        });
    }

    const ajouterBtn = document.getElementById('ajouter');
    if (ajouterBtn) {
        ajouterBtn.addEventListener('click', async function() {
            const medicament = document.getElementById('medicament').value.trim();
            const posologie = document.getElementById('posologie').value.trim();
            const quantite = document.getElementById('quantite').value.trim();

            if (!medicament) {
                return;
            }

            const meds = await chargerMedicaments();
            if (!meds.includes(medicament)) {
                sauvegarderMedicamentPersonnalise(medicament);
            }

            ordonnanceList.push({
                medicament: medicament,
                posologie: posologie,
                quantite: quantite
            });

            document.getElementById('medicament').value = '';
            document.getElementById('posologie').value = '';
            document.getElementById('quantite').value = '';

            await mettreAJourTableau();
        });
    }

    const genererPdfBtn = document.getElementById('generer-pdf');
    if (genererPdfBtn) {
        genererPdfBtn.addEventListener('click', function() {
            const headerOption = document.querySelector('input[name="header-option"]:checked').value;
            if (headerOption === 'DEM-header') {
                ordonnancedem('12');
            } else {
                ordonnance();
            }
        });
    }

    const enregistrerTypeBtn = document.getElementById('enregistrer-type');

    const rechercheOrdonnancesInput = document.getElementById('recherche-ordonnances-types');
    const selectOrdonnances = document.getElementById('liste-ordonnances-types');
    
    if (rechercheOrdonnancesInput && selectOrdonnances) {
        rechercheOrdonnancesInput.addEventListener('input', function() {
            const valeur = this.value.trim().toUpperCase();
            const options = selectOrdonnances.getElementsByTagName('option');
            
            let nbVisible = 0;
            
            for (let i = 0; i < options.length; i++) {
                const option = options[i];
                if (option.value === '') {
                    option.style.display = '';
                    continue;
                }
                
                if (valeur === '' || option.text.toUpperCase().startsWith(valeur)) {
                    option.style.display = '';
                    nbVisible++;
                } else {
                    option.style.display = 'none';
                }
            }

            if (valeur.length > 0) {
                selectOrdonnances.size = Math.min(5, nbVisible + 1);
            } else {
                selectOrdonnances.size = 1;
            }
        });

        rechercheOrdonnancesInput.addEventListener('focus', function() {
            selectOrdonnances.size = 5;
            rechercheOrdonnancesInput.focus();
        });

        selectOrdonnances.addEventListener('blur', function() {
            selectOrdonnances.size = 1;
        });
    }

    const chargerOrdonnanceTypeBtn = document.getElementById('charger-ordonnance-type');
    if (chargerOrdonnanceTypeBtn) {
        chargerOrdonnanceTypeBtn.addEventListener('click', async function() {
            const selectedType = selectOrdonnances.value;
            
            if (selectedType && ordonnancesTypesChargees[selectedType]) {
                ordonnanceList.length = 0;
                ordonnanceList.push(...ordonnancesTypesChargees[selectedType]);
                await mettreAJourTableau();
                
                if (rechercheOrdonnancesInput) {
                    rechercheOrdonnancesInput.value = '';
                    selectOrdonnances.size = 1;
                }
            }
        });
    }

    if (enregistrerTypeBtn) {
        enregistrerTypeBtn.addEventListener('click', function() {
            const nomType = prompt('Nom de cette ordonnance type:');
            if (nomType && ordonnanceList.length > 0) {
                let ordonnancesTypes = JSON.parse(localStorage.getItem('ordonnancesTypesPourOrd') || '{}');
                let sourcesData = JSON.parse(localStorage.getItem('ordonnancesTypesSources') || '{}');
                ordonnancesTypes[nomType] = ordonnanceList;
                sourcesData[nomType] = 'localStorage';
                localStorage.setItem('ordonnancesTypesPourOrd', JSON.stringify(ordonnancesTypes));
                localStorage.setItem('ordonnancesTypesSources', JSON.stringify(sourcesData));
                window.ordonnancesTypesSources = sourcesData;
                
                ordonnancesTypesChargees = ordonnancesTypes;
                mettreAJourListeOrdonnancesTypes(ordonnancesTypes);
                alert('Ordonnance type enregistrée !');
            }
        });
    }

    const supprimerOrdonnanceBtn = document.getElementById('supprimer-ordonnance');
    if (supprimerOrdonnanceBtn) {
        supprimerOrdonnanceBtn.addEventListener('click', function() {
            const select = document.getElementById('liste-ordonnances-types');
            const selectedType = select.value;
            
            if (selectedType && confirm('Voulez-vous vraiment supprimer cette ordonnance type ?')) {
                let ordonnancesTypes = JSON.parse(localStorage.getItem('ordonnancesTypes') || '{}');
                delete ordonnancesTypes[selectedType];
                localStorage.setItem('ordonnancesTypes', JSON.stringify(ordonnancesTypes));
                
                mettreAJourListeOrdonnancesTypes();
            }
        });
    }

    async function mettreAJourTableau() {
        const tbody = document.querySelector('#ordonnance-table tbody');
        tbody.innerHTML = '';

        const tousLesMeds = await chargerMedicaments();

        ordonnanceList.forEach((item, index) => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td>
                    <input type="text" class="med-input" value="${item.medicament}" list="medicaments-table-${index}" onchange="modifierMedicament(${index}, 'medicament', this.value)">
                    <datalist id="medicaments-table-${index}"></datalist>
                </td>
                <td><input type="text" class="poso-input" value="${item.posologie}" onchange="modifierMedicament(${index}, 'posologie', this.value)"></td>
                <td><input type="text" class="qt-input" value="${item.quantite}" onchange="modifierMedicament(${index}, 'quantite', this.value)"></td>
                <td class="no-print">
                    <button class="btn-danger" style="padding: 0.25rem 0.5rem; font-size: 0.75rem;" onclick="supprimerMedicament(${index})">
                        <i class="fas fa-trash"></i>
                    </button>
                </td>
            `;
            tbody.appendChild(tr);

            const medInput = tr.querySelector('.med-input');
            const dataList = tr.querySelector(`#medicaments-table-${index}`);
            
            tousLesMeds.forEach(med => {
                const option = document.createElement('option');
                option.value = med;
                dataList.appendChild(option);
            });

            medInput.addEventListener('input', function() {
                const valeur = this.value.trim().toUpperCase();
                
                if (valeur === '') {
                    dataList.innerHTML = '';
                    tousLesMeds.forEach(med => {
                        const option = document.createElement('option');
                        option.value = med;
                        dataList.appendChild(option);
                    });
                } else {
                    const medsFiltres = tousLesMeds.filter(med => 
                        med.toUpperCase().startsWith(valeur)
                    );
                    
                    dataList.innerHTML = '';
                    medsFiltres.forEach(med => {
                        const option = document.createElement('option');
                        option.value = med;
                        dataList.appendChild(option);
                    });
                }
            });
        });
    }

    window.modifierMedicament = async function(index, champ, valeur) {
        if (index >= 0 && index < ordonnanceList.length) {
            if (champ === 'medicament') {
                const meds = await chargerMedicaments();
                if (!meds.includes(valeur.trim())) {
                    sauvegarderMedicamentPersonnalise(valeur.trim());
                }
            }
            ordonnanceList[index][champ] = valeur.trim();
        }
    };

    window.supprimerMedicament = async function(index) {
        ordonnanceList.splice(index, 1);
        await mettreAJourTableau();
    };

    function afficherInfosEtablissement() {
        const polyclinique = localStorage.getItem('polyclinique') || '';
        const polycliniqueAr = localStorage.getItem('polyclinique-ar') || '';
        const docteur = localStorage.getItem('docteur') || '';

        document.getElementById('polyclinique').value = polyclinique;
        document.getElementById('polyclinique-ar').value = polycliniqueAr;
        document.getElementById('docteur').value = docteur;

        const affichagePolyclinique = document.getElementById('affichage-polyclinique');
        const affichageDocteur = document.getElementById('affichage-docteur');

        if (affichagePolyclinique) {
            affichagePolyclinique.innerHTML = `<p><strong>Polyclinique:</strong> ${polyclinique}</p>`;
        }
        if (affichageDocteur) {
            affichageDocteur.innerHTML = `<p><strong>Docteur:</strong> ${docteur}</p>`;
        }
    }

    function generateHeader() {
      return `
    <div id="head" style="border: 1px solid #000; padding: 10px; margin-bottom: 20px;">
        <table style="width: 100%;">
            <tbody>
                <tr>
                    <td colspan="4">
                        <div style="text-align: center; width: 100%;">REPUBLIQUE ALGERIENNE DEMOCRATIQUE ET POPULAIRE</div>
                    </td>
                </tr>
                <tr>
                    <td colspan="4">
                        <div style="text-align: center; width: 100%;">MINISTERE DE LA SANTE</div>
                    </td>
                </tr>
                <tr>
                    <td colspan="2">
                        <div style="width: 100%; font-size: 12px; white-space: pre-wrap;">${localStorage.getItem('polyclinique') || ''}</div>
                    </td>
                    <td colspan="2" style="text-align: right;">
                        <div style="text-align: right; width: 100%; font-size: 12px; white-space: pre-wrap;" class="arabic-text">${localStorage.getItem('polyclinique-ar') || ''}</div>
                    </td>
                </tr>
            </tbody>
        </table>
    </div>
    `;
    }

    function generateHeaderDem() {
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
                        <div style="width: 100%; font-size: 12px; white-space: pre-wrap;">${localStorage.getItem('polyclinique') || ''}</div>
                    </td>
                    <td colspan="2" style="text-align: right;">
                        <div style="text-align: right; width: 100%; font-size: 12px; white-space: pre-wrap;" class="arabic-text">${localStorage.getItem('polyclinique-ar') || ''}</div>
                    </td>
                </tr>
            </tbody>
        </table>
    </div>
    `;
    }

    function ordonnance() {
        const headerContent = generateHeader();
        const nom = capitalizeNames(document.getElementById("nom").value.trim());
        const prenom = capitalizeNames(document.getElementById("prenom").value.trim());
        const dateNaissance = document.getElementById("date-naissance").value;
        const age = document.getElementById("age").value;
        const numero = document.getElementById("numero").value;
        const poids = document.getElementById("poids").value.trim();

        const dateConsultation = document.querySelector('input[name="date-consultation"]').value;
        let formattedDate = '';
        if (dateConsultation) {
            const [year, month, day] = dateConsultation.split('-');
            formattedDate = `${day}/${month}/${year}`;
        } // Si la date n'est pas définie, on laisse formattedDate vide

        let itemsContent = '';
        const taillePolice = '12';

        ordonnanceList.forEach((item, index) => {
            const med = item.medicament;
            const poso = item.posologie;
            const qt = item.quantite;
            const nbr = index + 1;

            itemsContent += `
                <div style="margin-bottom: 15px;">
                    <div style="display: flex; justify-content: space-between; align-items: baseline;">
                        <span style="flex: 1;">
                            <span style="font-weight: bold; min-width: 20px; display: inline-block;" class="med-font">${nbr}.</span>
                            <span style="white-space: normal;" class="med-font">${med}</span>
                        </span>
                        <span style="margin-left: 20px; white-space: nowrap;" class="med-font">${qt}</span>
                    </div>
                    <div style="margin-left: 30px; color: #555; font-style: italic; margin-top: 5px;" class="med-font">${poso}</div>
                </div>`;
        });

        const avecEntete = document.querySelector('input[name="header-option"][value="with-header"]').checked;

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
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Ordonnance Medicale</title>
            <style>
                :root {
                    --med-font-size: 12px;
                }
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
                    position: absolute;
                }
                .info.nom { top: 60px; left: 20px; }
                .info.prenom { top: 90px; left: 20px; }
                .info.date-naissance { top: 120px; left: 20px; }
                .info.today { top: 90px; left: 270px; }
                .info.numero { top: 60px; left: 270px; }
                .info.poids { top: 120px; left: 270px; }
                .info.barcode { 
                    top: 20px; 
                    left: 420px;
                    display: none !important;
                }
                .info.barcode svg {
                    height: 50%;
                    width: 80%;
                }
                
                h1 {
                    text-align: center;
                    color: #333;
                    text-decoration: underline;
                    font-size: 20px;
                    margin: 10px 0 20px 0;
                }
                
                .medication-list {
                    margin-top: 150px;
                    margin-bottom: 20px;
                    margin-right: 14px;
                }
                
                .barcode-section {
                    text-align: center;
                    margin-top: 20px;
                }
                
                .barcode-section svg {
                    height: 50px;
                    width: 200px;
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
                        padding: 2px !important;
                        max-width: 100% !important;
                    }
                    
                    h1 {
                        font-size: 14pt !important;
                        margin: 5px 0 !important;
                    }
                    
                    .info {
                        font-size: 9pt !important;
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
                    
                    .medication-list span,
                    .medication-list div[style*="margin-left: 30px"] {
                        font-size: var(--med-font-size) !important;
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
                <h1>Ordonnance Medicale</h1>
                <div class="info nom"><strong>Nom :</strong> ${nom}</div>
                <div class="info prenom"><strong>Prenom :</strong> ${prenom}</div>
                <div class="info date-naissance"><strong>Date de naissance :</strong> ${dateNaissance} (${age})</div>
                <div class="info today"><strong>La date :</strong> ${formattedDate}</div>
                <div class="info numero"><strong>Numero :</strong> ${numero}</div>
                ${poids ? `<div class="info poids" style="top: 120px; left: 400px;"><strong>Poids :</strong> ${poids}</div>` : ''}
                <div class="info barcode">
                    <svg id="barcode" data-numero="${numero}"></svg>
                </div>
                <div class="medication-list">
                    ${itemsContent}
                </div>
            </div>
            <div class="print-button">
                <div style="display: flex; align-items: center; justify-content: center; gap: 10px; margin-bottom: 10px;">
                    <label for="numero-police-impression" style="font-size: 14px; font-weight: bold;">Taille police:</label>
                    <input type="number" id="numero-police-impression" min="8" max="24" value="14" style="width: 80px; padding: 5px; font-size: 14px; border: 1px solid #ccc; border-radius: 4px;">
                </div>
                <button id="printButton">Imprimer l'ordonnance</button>
            </div>
            
            <script src="https://cdn.jsdelivr.net/npm/jsbarcode@3.11.0/dist/JsBarcode.all.min.js"></script>
            <script>
                document.addEventListener('DOMContentLoaded', function() {
                    const printBtn = document.getElementById('printButton');
                    if (printBtn) {
                        printBtn.addEventListener('click', function() {
                            const fontSizeInput = document.getElementById('numero-police-impression');
                            if (fontSizeInput) {
                                const fontSize = fontSizeInput.value;
                                document.documentElement.style.setProperty('--med-font-size', fontSize + 'px');
                                document.querySelectorAll('.med-font').forEach(el => {
                                    el.style.fontSize = fontSize + 'px';
                                });
                            }
                            window.print();
                        });
                    }
                    
                    const barcode = document.getElementById('barcode');
                    if (barcode && typeof JsBarcode !== 'undefined') {
                        const numero = barcode.getAttribute('data-numero');
                        if (numero && numero !== '0' && numero !== '') {
                            try {
                                JsBarcode("#barcode", numero, {
                                    format: "CODE128",
                                    width: 2,
                                    height: 50,
                                    displayValue: false
                                });
                            } catch (e) {
                                console.log('Erreur génération barcode:', e);
                            }
                        }
                    }
                });
            </script>
        </body>
        </html>`;

        if (typeof openWindowWithContent === 'function') {
            openWindowWithContent(certificatContent, 'certificat_window');
        } else {
            const newWindow = window.open("", "_blank");
            if (newWindow) {
                safeDocumentWrite(newWindow, certificatContent);
            } else {
                alert("Le popup a ete bloque par votre navigateur. Veuillez autoriser les popups pour ce site.");
            }
        }
    }

    function ordonnancedem(taillePolice = '14') {
        const nom = capitalizeNames(document.getElementById("nom").value.trim());
        const prenom = capitalizeNames(document.getElementById("prenom").value.trim());
        const dateNaissance = document.getElementById("date-naissance").value;
        const age = document.getElementById("age").value;
        const numero = document.getElementById("numero").value;
        const poids = document.getElementById("poids").value.trim();

        const dateConsultation = document.querySelector('input[name="date-consultation"]').value;
        let formattedDate = '';
        if (dateConsultation) {
            const [year, month, day] = dateConsultation.split('-');
            formattedDate = `${day}/${month}/${year}`;
        } // Si la date n'est pas définie, on laisse formattedDate vide

        let itemsContent = '';

        ordonnanceList.forEach((item, index) => {
            const med = item.medicament;
            const poso = item.posologie;
            const qt = item.quantite;
            const nbr = index + 1;

            itemsContent += `
                <div style="margin-bottom: 15px;">
                    <div style="display: flex; justify-content: space-between; align-items: baseline;">
                        <span style="flex: 1;">
                            <span style="font-weight: bold; min-width: 20px; display: inline-block;" class="med-font">${nbr}.</span>
                            <span style="white-space: normal;" class="med-font">${med}</span>
                        </span>
                        <span style="margin-left: 20px; white-space: nowrap;" class="med-font">${qt}</span>
                    </div>
                    <div style="margin-left: 30px; color: #555; font-style: italic; margin-top: 5px;" class="med-font">${poso}</div>
                </div>`;
        });

        const enteteContent = generateHeaderDem();

        const certificatContent = `
        <!DOCTYPE html>
        <html lang="fr">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Ordonnance Medicale</title>
            <style>
                :root {
                    --med-font-size: 12px;
                }
                body {
                    font-family: Arial, sans-serif;
                    padding: 20px;
                    background-color: #f9f9f9;
                }
                .certificat {
                    background-color: white;
                    border: 1px solid #ddd;
                    padding: 200px 20px 20px 20px;
                    max-width: 600px;
                    margin: 0 auto;
                    margin-top: 60px;
                    box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
                    position: relative;
                }
                .info {
                    position: absolute;
                }
                .info.nom { top: 10px; left: 20px; }
                .info.prenom { top: 40px; left: 20px; }
                .info.date-naissance { top: 70px; left: 20px; }
                .info.today { top: 40px; left: 270px; }
                .info.numero { 
                    top: 10px; 
                    left: 270px; 
                    color: transparent;
                }
                .info.poids { top: 70px; left: 270px; }
                .info.barcode {
                    top: -150px;
                    left: 420px;
                    display: none !important;
                }
                .info.barcode svg {
                    height: 50%;
                    width: 80%;
                }
                h1 {
                    text-align: center;
                    color: #333;
                    text-decoration: underline;
                    font-size: 20px;
                    margin-top: 80px;
                }
                .medication-list {
                    margin-top: 5px;
                    margin-bottom: 20px;
                    margin-right: 14px;
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
                        padding: 2px !important;
                        max-width: 100% !important;
                    }
                    
                    h1 {
                        font-size: 14pt !important;
                        margin: 5px 0 !important;
                    }
                    
                    .info {
                        font-size: 9pt !important;
                    }
                    
                    .info.numero {
                        color: inherit !important;
                    }
                    
                    .info.barcode {
                        display: block !important;
                        top: -30px !important;
                        left: 420px !important;
                    }
                    
                    .info.barcode svg {
                        height: 30px !important;
                        width: 150px !important;
                    }
                    
                    .medication-list {
                        margin-top: 120px !important;
                        margin-bottom: 10px !important;
                        margin-right: 14px !important;
                    }
                    
                    div[style*="margin-bottom: 15px"] {
                        margin-bottom: 5px !important;
                    }
                    
                    .medication-list span,
                    .medication-list div[style*="margin-left: 30px"] {
                        font-size: var(--med-font-size) !important;
                    }
                    
                    .print-button {
                        display: none;
                    }
                }
            </style>
        </head>
        <body>
            ${enteteContent}
            <div style="
                width: calc(100% - 5px);
                text-align: center;
                border: 1px solid #000;
                padding: 5px 0;
                font-size: 20px;
                font-weight: normal;
                margin-top: 20px;
                margin-bottom: 5px;
                margin-right: 10px;
                background-color: transparent;
            ">
                ORDONNANCE
            </div>
            <div class="certificat">
                <div class="info nom"><strong>Nom :</strong> ${nom}</div>
                <div class="info prenom"><strong>Prenom :</strong> ${prenom}</div>
                <div class="info date-naissance"><strong>Date de naissance :</strong> ${dateNaissance} (${age})</div>
                <div class="info today"><strong>La date :</strong> ${formattedDate}</div>
                <div class="info numero"><strong>Numero :</strong> ${numero}</div>
                ${poids ? `<div class="info poids" style="top: 70px; left: 400px;"><strong>Poids :</strong> ${poids}</div>` : ''}
                <div class="info barcode">
                    <svg id="barcode" data-numero="${numero || '0'}"></svg>
                </div>
                <div class="medication-list">
                    ${itemsContent}
                </div>
            </div>
            <div class="print-button">
                <div style="display: flex; align-items: center; justify-content: center; gap: 10px; margin-bottom: 10px;">
                    <label for="numero-police-impression" style="font-size: 14px; font-weight: bold;">Taille police:</label>
                    <select id="numero-police-impression" style="width: 80px; padding: 5px; font-size: 14px; border: 1px solid #ccc; border-radius: 4px;">
                        <option value="10">10</option>
                        <option value="11">11</option>
                        <option value="12" selected>12</option>
                        <option value="14">14</option>
                        <option value="16">16</option>
                        <option value="18">18</option>
                        <option value="20">20</option>
                        <option value="22">22</option>
                    </select>
                </div>
                <button id="printButton">Imprimer l'ordonnance</button>
            </div>
            <script src="https://cdn.jsdelivr.net/npm/jsbarcode@3.11.0/dist/JsBarcode.all.min.js"></script>
            <script>
                document.addEventListener('DOMContentLoaded', function() {
                    const printBtn = document.getElementById('printButton');
                    if (printBtn) {
                        printBtn.addEventListener('click', function() {
                            const fontSizeSelect = document.getElementById('numero-police-impression');
                            if (fontSizeSelect) {
                                const fontSize = fontSizeSelect.value;
                                document.documentElement.style.setProperty('--med-font-size', fontSize + 'px');
                                document.querySelectorAll('.med-font').forEach(el => {
                                    el.style.fontSize = fontSize + 'px';
                                });
                            }
                            window.print();
                        });
                    }
                    
                    const barcode = document.getElementById('barcode');
                    if (barcode && typeof JsBarcode !== 'undefined') {
                        const numero = barcode.getAttribute('data-numero');
                        if (numero && numero !== '0' && numero !== '') {
                            try {
                                JsBarcode("#barcode", numero, {
                                    format: "CODE128",
                                    width: 2,
                                    height: 50,
                                    displayValue: false
                                });
                            } catch (e) {
                                console.log('Erreur génération barcode:', e);
                            }
                        }
                    }
                });
            </script>
        </body>
        </html>`;

        if (typeof openWindowWithContent === 'function') {
            openWindowWithContent(certificatContent, 'certificat_window_2');
        } else {
            const newWindow = window.open("", "_blank");
            if (newWindow) {
                safeDocumentWrite(newWindow, certificatContent);
            } else {
                alert("Le popup a ete bloque par votre navigateur. Veuillez autoriser les popups pour ce site.");
            }
        }
    }

    async function ajouterMedicamentALaListe(medicament) {
        ordonnanceList.push({
            medicament: medicament,
            posologie: '',
            quantite: ''
        });
        await mettreAJourTableau();
    }

    function genererCertificat(estDeces) {
        const nom = document.getElementById('nom').value;
        const prenom = document.getElementById('prenom').value;
        const dateNaissance = document.getElementById('date-naissance').value;
        const dateConsultation = document.querySelector('input[name="date-consultation"]').value;
        
        const docteur = localStorage.getItem('docteur') || '';
        const polyclinique = localStorage.getItem('polyclinique') || '';
        
        let typeCertificat = estDeces ? 'CERTIFICAT DE DÉCÈS' : 'CERTIFICAT MÉDICAL';
        let contenu = '';
        
        // Format the dateConsultation properly
        let formattedDate = '';
        if (dateConsultation) {
            const [year, month, day] = dateConsultation.split('-');
            formattedDate = `${day}/${month}/${year}`;
        } // Si la date n'est pas définie, on laisse formattedDate vide
        
        if (estDeces) {
            contenu = `Je soussigné, Dr ${docteur}, certifie que M/Mme ${nom} ${prenom}, né(e) le ${dateNaissance}, est décédé(e) le ${formattedDate}.`;
        } else {
            contenu = `Je soussigné, Dr ${docteur}, certifie avoir examiné ce jour M/Mme ${nom} ${prenom}, né(e) le ${dateNaissance}.`;
        }

        const newWindow = window.open('', '_blank');
        newWindow.document.write(`
            <!DOCTYPE html>
            <html>
            <head>
                <title>${typeCertificat}</title>
                <style>
                    body { font-family: Arial, sans-serif; max-width: 800px; margin: 50px auto; padding: 20px; }
                    h1 { text-align: center; margin-bottom: 40px; }
                    .header { text-align: center; margin-bottom: 30px; }
                    .content { line-height: 1.6; font-size: 16px; }
                    .footer { margin-top: 50px; text-align: right; }
                    button { margin-top: 20px; padding: 10px 20px; }
                </style>
            </head>
            <body>
                <div class="header">
                    <h2>${polyclinique}</h2>
                    <h1>${typeCertificat}</h1>
                </div>
                <div class="content">
                    <p>${contenu}</p>
                </div>
                <div class="footer">
                    <p>Fait le ${formattedDate}</p>
                    <p><strong>Dr ${docteur}</strong></p>
                    <p>Signature et cachet</p>
                </div>
                <button onclick="window.print()">Imprimer</button>
            </body>
            </html>
        `);
        newWindow.document.close();
    }

    function chargerMedicaments() {
        return JSON.parse(localStorage.getItem('medicaments') || '[]');
    }

    function chargerMedicamentsJSON() {
        if (typeof medicamentsData !== 'undefined' && Array.isArray(medicamentsData)) {
            return Promise.resolve(medicamentsData);
        }
        return Promise.resolve([]);
    }

    async function chargerMedicaments() {
        const medsJSON = await chargerMedicamentsJSON();
        const medsLocalStorage = JSON.parse(localStorage.getItem('medicamentsPersonnalises') || '[]');
        
        const tousLesMeds = [...new Set([...medsJSON, ...medsLocalStorage])];
        return tousLesMeds;
    }

    function sauvegarderMedicamentPersonnalise(medicament) {
        let medsPersonnalises = JSON.parse(localStorage.getItem('medicamentsPersonnalises') || '[]');
        if (!medsPersonnalises.includes(medicament)) {
            medsPersonnalises.push(medicament);
            localStorage.setItem('medicamentsPersonnalises', JSON.stringify(medsPersonnalises));
        }
    }

    async     function remplirDatalistMedicaments() {
        const datalist = document.getElementById('medicaments');
        const meds = await chargerMedicaments();
        
        datalist.innerHTML = '';
        meds.forEach(med => {
            const option = document.createElement('option');
            option.value = med;
            datalist.appendChild(option);
        });

        const medicamentInput = document.getElementById('medicament');
        if (medicamentInput) {
            medicamentInput.addEventListener('input', async function() {
                const valeur = this.value.trim().toUpperCase();
                const datalist = document.getElementById('medicaments');
                const tousLesMeds = await chargerMedicaments();
                
                if (valeur === '') {
                    datalist.innerHTML = '';
                    tousLesMeds.forEach(med => {
                        const option = document.createElement('option');
                        option.value = med;
                        datalist.appendChild(option);
                    });
                } else {
                    const medsFiltres = tousLesMeds.filter(med => 
                        med.toUpperCase().startsWith(valeur)
                    );
                    
                    datalist.innerHTML = '';
                    medsFiltres.forEach(med => {
                        const option = document.createElement('option');
                        option.value = med;
                        datalist.appendChild(option);
                    });
                }
            });

            medicamentInput.addEventListener('keydown', async function(e) {
                if (e.key === 'Enter') {
                    e.preventDefault();
                    const medicament = this.value.trim();
                    if (!medicament) {
                        return;
                    }
                    
                    const meds = await chargerMedicaments();
                    if (!meds.includes(medicament)) {
                        sauvegarderMedicamentPersonnalise(medicament);
                    }
                    
                    ordonnanceList.push({
                        medicament: medicament,
                        posologie: document.getElementById('posologie').value.trim(),
                        quantite: document.getElementById('quantite').value.trim()
                    });
                    this.value = '';
                    document.getElementById('posologie').value = '';
                    document.getElementById('quantite').value = '';
                    await mettreAJourTableau();
                }
            });
        }
    }

    async function validerMedicament(medicament) {
        const meds = await chargerMedicamentsJSON();
        const medicamentNormalise = medicament.trim().toUpperCase();
        return meds.some(med => med.toUpperCase() === medicamentNormalise);
    }

    function genererOrdonnance() {
        const nom = document.getElementById('nom').value;
        const prenom = document.getElementById('prenom').value;
        const dateNaissance = document.getElementById('date-naissance').value;
        const numero = document.getElementById('numero').value;
        const poids = document.getElementById('poids').value;
        
        const docteur = localStorage.getItem('docteur') || 'Dr Daoudi';
        const polyclinique = localStorage.getItem('polyclinique') || '';
        const polycliniqueAr = localStorage.getItem('polyclinique-ar') || '';
        
        const dateConsultation = document.querySelector('input[name="date-consultation"]').value;
        const dateCourante = dateConsultation || new Date().toLocaleDateString('fr-FR');

        if (!nom || !prenom) {
            alert('Veuillez remplir au moins le nom et le prénom du patient');
            return;
        }

        if (ordonnanceList.length === 0) {
            alert('Veuillez ajouter au moins un médicament à l\'ordonnance');
            return;
        }

        const headerOption = document.querySelector('input[name="header-option"]:checked').value;
        
        let headerHTML = '';
        if (headerOption === 'with-header' || headerOption === 'DEM-header') {
            headerHTML = `
                <div class="ordonnance-header">
                    <div class="header-top">
                        <div class="polyclinique-fr">${polyclinique}</div>
                        <div class="docteur-info">Dr ${docteur}</div>
                    </div>
                    ${polycliniqueAr ? `<div class="polyclinique-ar" dir="rtl">${polycliniqueAr}</div>` : ''}
                </div>
            `;
        }

        const medicamentsHTML = ordonnanceList.map((med, index) => `
            <div class="medication-row">
                <div class="medication-number">${index + 1}.</div>
                <div class="medication-info">
                    <div class="medication-name"><strong>${med.medicament}</strong></div>
                    ${med.posologie ? `<div class="medication-posologie">Posologie: ${med.posologie}</div>` : ''}
                    ${med.quantite ? `<div class="medication-quantite">Quantité: ${med.quantite}</div>` : ''}
                </div>
            </div>
        `).join('');

        const newWindow = window.open('', '_blank', 'width=800,height=1000');
        newWindow.document.write(`
            <!DOCTYPE html>
            <html lang="fr">
            <head>
                <meta charset="UTF-8">
                <title>Ordonnance - ${nom} ${prenom}</title>
                <style>
                    @page {
                        size: A5;
                        margin: 1cm;
                    }
                    * {
                        margin: 0;
                        padding: 0;
                        box-sizing: border-box;
                    }
                    body {
                        font-family: 'Times New Roman', serif;
                        font-size: 12pt;
                        line-height: 1.4;
                        color: #000;
                        background: white;
                        padding: 20px;
                    }
                    .ordonnance-container {
                        max-width: 700px;
                        margin: 0 auto;
                    }
                    .ordonnance-header {
                        text-align: center;
                        margin-bottom: 30px;
                        padding-bottom: 15px;
                        border-bottom: 2px solid #000;
                    }
                    .header-top {
                        margin-bottom: 10px;
                    }
                    .polyclinique-fr {
                        font-size: 14pt;
                        font-weight: bold;
                        margin-bottom: 5px;
                    }
                    .polyclinique-ar {
                        font-size: 14pt;
                        font-weight: bold;
                        direction: rtl;
                        margin-bottom: 10px;
                    }
                    .docteur-info {
                        font-size: 13pt;
                        font-style: italic;
                    }
                    .titre-ordonnance {
                        text-align: center;
                        font-size: 16pt;
                        font-weight: bold;
                        margin: 20px 0;
                        text-transform: uppercase;
                        text-decoration: underline;
                    }
                    .patient-info {
                        background: #f9f9f9;
                        padding: 10px;
                        margin-bottom: 20px;
                        border-left: 3px solid #000;
                    }
                    .patient-info p {
                        margin: 3px 0;
                    }
                    .patient-info strong {
                        font-weight: bold;
                    }
                    .date-consultation {
                        text-align: right;
                        margin-bottom: 20px;
                        font-style: italic;
                    }
                    .medication-list {
                        margin: 20px 0;
                    }
                    .medication-row {
                        display: flex;
                        margin-bottom: 15px;
                        padding: 10px;
                        background: white;
                        border-bottom: 1px solid #ddd;
                    }
                    .medication-number {
                        font-weight: bold;
                        min-width: 30px;
                        font-size: 14pt;
                    }
                    .medication-info {
                        flex: 1;
                    }
                    .medication-name {
                        font-size: 13pt;
                        font-weight: bold;
                        margin-bottom: 5px;
                    }
                    .medication-posologie {
                        font-size: 11pt;
                        color: #333;
                        margin-bottom: 3px;
                    }
                    .medication-quantite {
                        font-size: 11pt;
                        color: #666;
                    }
                    .signature-section {
                        margin-top: 50px;
                        text-align: right;
                    }
                    .signature-line {
                        display: inline-block;
                        width: 200px;
                        border-top: 1px solid #000;
                        padding-top: 10px;
                        text-align: center;
                    }
                    .date-signature {
                        margin-top: 20px;
                        text-align: right;
                    }
                    .print-button {
                        position: fixed;
                        top: 10px;
                        right: 10px;
                        padding: 10px 20px;
                        background: #2563eb;
                        color: white;
                        border: none;
                        border-radius: 5px;
                        cursor: pointer;
                        font-size: 14px;
                        z-index: 1000;
                    }
                    .print-button:hover {
                        background: #1d4ed8;
                    }
                    @media print {
                        .print-button {
                            display: none !important;
                        }
                        body {
                            padding: 0;
                        }
                        .ordonnance-container {
                            max-width: 100%;
                        }
                    }
                </style>
            </head>
            <body>
                <button class="print-button" onclick="window.print()">📄 Imprimer / Enregistrer PDF</button>
                
                <div class="ordonnance-container">
                    ${headerHTML}
                    
                    <div class="titre-ordonnance">Ordonnance Médicale</div>
                    
                    <div class="patient-info">
                        <p><strong>Nom:</strong> ${nom}</p>
                        <p><strong>Prénom:</strong> ${prenom}</p>
                        ${dateNaissance ? `<p><strong>Date de naissance:</strong> ${dateNaissance}</p>` : ''}
                        ${numero ? `<p><strong>Numéro Patient:</strong> ${numero}</p>` : ''}
                        ${poids ? `<p><strong>Poids:</strong> ${poids} kg</p>` : ''}
                    </div>
                    
                    <div class="date-consultation">Fait le ${dateCourante}</div>
                    
                    <div class="medication-list">
                        <h3 style="margin-bottom: 15px; text-decoration: underline;">Prescription:</h3>
                        ${medicamentsHTML}
                    </div>
                    
                    <div class="signature-section">
                        <div class="signature-line">
                            <strong>Dr ${docteur}</strong>
                        </div>
                        <div class="date-signature">Signature et Cachet</div>
                    </div>
                </div>
                
                <script>
                    setTimeout(function() {
                        window.focus();
                    }, 100);
                </script>
            </body>
            </html>
        `);
        newWindow.document.close();
    }

    function calculerAge(dateNaissance) {
        if (!dateNaissance) return '';
        
        const aujourdhui = new Date();
        const naissance = new Date(dateNaissance);
        
        const differenceTemps = aujourdhui - naissance;
        const differenceJours = Math.floor(differenceTemps / (1000 * 60 * 60 * 24));
        
        if (differenceJours >= 365) {
            let ans = Math.floor(differenceJours / 365);
            return ans + ' an' + (ans > 1 ? 's' : '');
        } else if (differenceJours >= 30) {
            let mois = Math.floor(differenceJours / 30);
            return mois + ' mois';
        } else {
            return differenceJours + ' jour' + (differenceJours > 1 ? 's' : '');
        }
    }

    const dateNaissanceInput = document.getElementById('date-naissance');
    const ageInput = document.getElementById('age');
    
    if (dateNaissanceInput && ageInput) {
        dateNaissanceInput.addEventListener('input', function() {
            const dateNaissance = this.value;
            const age = calculerAge(dateNaissance);
            ageInput.value = age;
        });
        
        dateNaissanceInput.addEventListener('change', function() {
            const dateNaissance = this.value;
            const age = calculerAge(dateNaissance);
            ageInput.value = age;
        });
    }

    afficherInfosEtablissement();
    await remplirDatalistMedicaments();
});

function chargerOrdonnancesTypesDuFichier() {
    const fromLocalStorage = JSON.parse(localStorage.getItem('ordonnancesTypesPourOrd') || '{}');
    
    console.log('🔄 Chargement des ordonnances types...');
    console.log('- Ordonnances dans localStorage :', Object.keys(fromLocalStorage).length);
    
    if (typeof ordonnancesTypesData !== 'undefined' && Object.keys(ordonnancesTypesData).length > 0) {
        console.log('✅ Ordonnances chargées depuis ordonnances-types-data.js !');
        console.log('- Ordonnances dans le fichier JS :', Object.keys(ordonnancesTypesData).length);
        
        const sourcesData = JSON.parse(localStorage.getItem('ordonnancesTypesSources') || '{}');
        const fusion = { ...ordonnancesTypesData };
        let nbAjoutes = 0;
        let nbRemplaces = 0;
        
        Object.keys(ordonnancesTypesData).forEach(nom => {
            sourcesData[nom] = 'file';
        });
        
        Object.keys(fromLocalStorage).forEach(nom => {
            if (!fusion[nom]) {
                fusion[nom] = fromLocalStorage[nom];
                sourcesData[nom] = 'localStorage';
                nbAjoutes++;
            } else {
                fusion[nom] = fromLocalStorage[nom];
                nbRemplaces++;
            }
        });
        
        localStorage.setItem('ordonnancesTypesSources', JSON.stringify(sourcesData));
        window.ordonnancesTypesSources = sourcesData;
        
        ordonnancesTypesChargees = fusion;
        localStorage.setItem('ordonnancesTypesPourOrd', JSON.stringify(fusion));
        mettreAJourListeOrdonnancesTypes(fusion);
        
        console.log('✅ Fusion terminée !');
        console.log('- Ordonnances du fichier JS :', Object.keys(ordonnancesTypesData).length);
        console.log('- Ordonnances personnalisées (localStorage) :', Object.keys(fromLocalStorage).length);
        console.log('- Ordonnances ajoutées :', nbAjoutes);
        console.log('- Ordonnances remplacées :', nbRemplaces);
        console.log('- Total final :', Object.keys(fusion).length);
    } else {
        console.log('⚠️ Impossible de charger ordonnances-types-data.js');
        console.log('💡 Utilisation des données localStorage uniquement...');
        if (Object.keys(fromLocalStorage).length > 0) {
            ordonnancesTypesChargees = fromLocalStorage;
            const sourcesData = {};
            Object.keys(fromLocalStorage).forEach(nom => {
                sourcesData[nom] = 'localStorage';
            });
            localStorage.setItem('ordonnancesTypesSources', JSON.stringify(sourcesData));
            window.ordonnancesTypesSources = sourcesData;
            mettreAJourListeOrdonnancesTypes(fromLocalStorage);
            console.log('✅ Liste chargée depuis localStorage :', Object.keys(fromLocalStorage).length, 'ordonnances');
        } else {
            console.log('⚠️ Aucune ordonnance disponible !');
            console.log('💡 Utilisez le bouton "Charger fichier pour ord.html" dans options.html pour charger vos ordonnances.');
        }
    }
}
