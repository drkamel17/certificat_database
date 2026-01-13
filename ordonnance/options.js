let contenuJSON = [];

// Créer un canal de communication pour notifier les autres onglets (si supporté)
const channel = typeof BroadcastChannel !== 'undefined' ? new BroadcastChannel('ordonnances_storage') : null;

// Simuler chrome.storage avec localStorage
const storage = {
    get: function(key, callback) {
        if (typeof key === 'string') {
            const data = localStorage.getItem(key);
            callback({ [key]: data ? JSON.parse(data) : undefined });
        } else {
            const result = {};
            Object.keys(key).forEach(k => {
                const data = localStorage.getItem(k);
                result[k] = data ? JSON.parse(data) : undefined;
            });
            callback(result);
        }
    },
    set: function(data, callback) {
        Object.keys(data).forEach(key => {
            localStorage.setItem(key, JSON.stringify(data[key]));
        });
        if (callback) callback();
    }
};

// === Chargement de la page ===
 document.addEventListener("DOMContentLoaded", async () => {
    // === Exporter ordonnances types ===
    document.getElementById("exporter-ordonnances-types").addEventListener("click", function() {
        const ordonnancesTypes = JSON.parse(localStorage.getItem('ordonnancesTypesPourOrd') || '{}');

        if (Object.keys(ordonnancesTypes).length === 0) {
            showMessage("Aucune ordonnance type à exporter.", "red");
            return;
        }

        const dataStr = JSON.stringify(ordonnancesTypes, null, 2);
        const dataUri = 'data:application/json;charset=utf-8,' + encodeURIComponent(dataStr);

        const exportFileDefaultName = 'ordonnances-types.json';

        const linkElement = document.createElement('a');
        linkElement.setAttribute('href', dataUri);
        linkElement.setAttribute('download', exportFileDefaultName);
        linkElement.click();

        showMessage(`${Object.keys(ordonnancesTypes).length} ordonnance(s) type(s) exportée(s) avec succès.`, "green");
    });

    // === Importer ordonnances types ===
    const btnImporterOrdonnancesTypes = document.getElementById("importer-ordonnances-types");
    const importOrdonnancesTypesInput = document.getElementById("fichier-ordonnances-type");

    if (btnImporterOrdonnancesTypes && importOrdonnancesTypesInput) {
        btnImporterOrdonnancesTypes.addEventListener("click", function() {
            importOrdonnancesTypesInput.click();
        });

        importOrdonnancesTypesInput.addEventListener("change", function(e) {
            const file = e.target.files[0];
            if (!file) return;

            const reader = new FileReader();
            reader.onload = function(e) {
                try {
                    const importedOrdonnances = JSON.parse(e.target.result);

                    if (typeof importedOrdonnances !== 'object' || Array.isArray(importedOrdonnances)) {
                        showMessage("Format de fichier invalide. Le fichier doit contenir un objet d'ordonnances types.", "red");
                        return;
                    }

                    localStorage.setItem('ordonnancesTypesPourOrd', JSON.stringify(importedOrdonnances));
                    localStorage.setItem('ordonnancesTypes', JSON.stringify(importedOrdonnances));

                    showMessage(`${Object.keys(importedOrdonnances).length} ordonnance(s) type(s) importée(s) avec succès.`, "green");
                    importOrdonnancesTypesInput.value = '';

                    // Recharger la liste pour afficher les ordonnances importées
                    chargerOrdonnancesTypes();

                    // Avertissement pour Firefox
                    const isFirefox = navigator.userAgent.toLowerCase().includes('firefox');
                    const isFileProtocol = window.location.protocol === 'file:';
                    if (isFirefox && isFileProtocol) {
                        setTimeout(() => {
                            showMessage("⚠️ Firefox : Cliquez sur le bouton d'actualisation dans ord.html pour voir les nouvelles ordonnances.", "#856404");
                        }, 1000);
                    }
                } catch (error) {
                    showMessage("Erreur lors de l'import : " + error.message, "red");
                }
            };
            reader.readAsText(file);
        });
    }

    // Écouteurs pour la gestion des ordonnances types
    document.getElementById("enregistrer-ordonnance").addEventListener("click", enregistrerOrdonnance);
    document.getElementById("annuler-ordonnance").addEventListener("click", annulerOrdonnance);
    document.getElementById("supprimer-ordonnance").addEventListener("click", supprimerOrdonnance);
    document.getElementById("ajouter-medicament").addEventListener("click", ajouterMedicamentForm);
    document.getElementById("recharger-ordonnances-types").addEventListener("click", chargerOrdonnancesTypes);

    // === Exporter médicaments personnalisés ===
    document.getElementById("exporter-medicaments-personnalises").addEventListener("click", function() {
        const medsPersonnalises = JSON.parse(localStorage.getItem('medicamentsPersonnalises') || '[]');

        if (medsPersonnalises.length === 0) {
            showMessage("Aucun médicament personnalisé à exporter.", "red");
            return;
        }

        const dataStr = JSON.stringify(medsPersonnalises, null, 2);
        const dataUri = 'data:application/json;charset=utf-8,' + encodeURIComponent(dataStr);

        const exportFileDefaultName = 'medicaments-personnalises.json';

        const linkElement = document.createElement('a');
        linkElement.setAttribute('href', dataUri);
        linkElement.setAttribute('download', exportFileDefaultName);
        linkElement.click();

        showMessage(`Export réussi : ${medsPersonnalises.length} médicament(s) exporté(s).`, "green");
    });

    // === Importer médicaments personnalisés ===
    const btnImporterMeds = document.getElementById("btn-importer-medicaments");
    const importMedsInput = document.getElementById("importer-medicaments-personnalises");

    // === Exporter ordonnances archivées ===
    document.getElementById("exporter-ordonnances-archivees").addEventListener("click", function() {
        const ordonnancesArchivees = JSON.parse(localStorage.getItem('ordonnancesPatients') || '{}');

        if (Object.keys(ordonnancesArchivees).length === 0) {
            showMessage("Aucune ordonnance archivée à exporter.", "red");
            return;
        }

        const dataStr = JSON.stringify(ordonnancesArchivees, null, 2);
        const dataUri = 'data:application/json;charset=utf-8,' + encodeURIComponent(dataStr);

        const exportFileDefaultName = 'ordonnances-archivees.json';

        const linkElement = document.createElement('a');
        linkElement.setAttribute('href', dataUri);
        linkElement.setAttribute('download', exportFileDefaultName);
        linkElement.click();

        showMessage(`${Object.keys(ordonnancesArchivees).length} ordonnance(s) archivée(s) exportée(s) avec succès.`, "green");
    });

    // === Importer ordonnances archivées ===
    const btnImporterOrdonnances = document.getElementById("btn-importer-ordonnances-archivees");
    const importOrdonnancesInput = document.getElementById("importer-ordonnances-archivees");

    if (btnImporterOrdonnances && importOrdonnancesInput) {
        btnImporterOrdonnances.addEventListener("click", function() {
            importOrdonnancesInput.click();
        });

        importOrdonnancesInput.addEventListener("change", function(e) {
            const file = e.target.files[0];
            if (!file) return;

            const reader = new FileReader();
            reader.onload = function(e) {
                try {
                    const importedOrdonnances = JSON.parse(e.target.result);

                    if (typeof importedOrdonnances !== 'object' || Array.isArray(importedOrdonnances)) {
                        showMessage("Format de fichier invalide. Le fichier doit contenir un objet d'ordonnances archivées.", "red");
                        return;
                    }

                    let ordonnancesArchivees = JSON.parse(localStorage.getItem('ordonnancesPatients') || '{}');
                    let nbAjoutes = 0;
                    let nbMisesAJour = 0;

                    Object.keys(importedOrdonnances).forEach(patientName => {
                        if (ordonnancesArchivees[patientName]) {
                            // Si le patient existe déjà, on met à jour
                            ordonnancesArchivees[patientName] = importedOrdonnances[patientName];
                            nbMisesAJour++;
                        } else {
                            // Sinon on ajoute le nouveau patient
                            ordonnancesArchivees[patientName] = importedOrdonnances[patientName];
                            nbAjoutes++;
                        }
                    });

                    // Sauvegarder les données
                    localStorage.setItem('ordonnancesPatients', JSON.stringify(ordonnancesArchivees));
                    console.log('Données stockées:', localStorage.getItem('ordonnancesPatients'));
                    
                    // Envoyer un message via BroadcastChannel pour notifier les autres onglets
                    if (channel) {
                        channel.postMessage({
                            type: 'ordonnancesPatientsUpdated',
                            data: ordonnancesArchivees
                        });
                        console.log('Message envoyé via BroadcastChannel');
                    }
                    
                    // L'événement 'storage' est automatiquement déclenché par le navigateur dans les autres onglets/fenêtres

                    let message = "";
                    if (nbAjoutes > 0) {
                        message += `${nbAjoutes} ordonnance(s) archivée(s) ajoutée(s). `;
                    }
                    if (nbMisesAJour > 0) {
                        message += `${nbMisesAJour} ordonnance(s) archivée(s) mise(s) à jour. `;
                    }
                    if (message === "") {
                        message = "Aucune donnée à importer.";
                    }
                    showMessage(message.trim(), "green");
                    
                    // Avertissement pour Firefox
                    const isFirefox = navigator.userAgent.toLowerCase().includes('firefox');
                    const isFileProtocol = window.location.protocol === 'file:';
                    if (isFirefox && isFileProtocol) {
                        setTimeout(() => {
                            showMessage("⚠️ Firefox : Cliquez sur le bouton d'actualisation dans ord.html pour voir les nouvelles ordonnances.", "#856404");
                        }, 1000);
                    }
                    
                    importOrdonnancesInput.value = '';
                } catch (error) {
                    showMessage("Erreur lors de l'import : " + error.message, "red");
                }
            };
            reader.readAsText(file);
        });
    }

    if (btnImporterMeds && importMedsInput) {
        btnImporterMeds.addEventListener("click", function() {
            importMedsInput.click();
        });

        importMedsInput.addEventListener("change", function(e) {
            const file = e.target.files[0];
            if (!file) return;

            const reader = new FileReader();
            reader.onload = function(e) {
                try {
                    const importedMeds = JSON.parse(e.target.result);

                    if (!Array.isArray(importedMeds)) {
                        showMessage("Format de fichier invalide. Le fichier doit contenir un tableau de médicaments.", "red");
                        return;
                    }

                    let medsPersonnalises = JSON.parse(localStorage.getItem('medicamentsPersonnalises') || '[]');
                    let nbAjoutes = 0;

                    importedMeds.forEach(med => {
                        if (typeof med === 'string' && med.trim() !== '' && !medsPersonnalises.includes(med.trim())) {
                            medsPersonnalises.push(med.trim());
                            nbAjoutes++;
                        }
                    });

                    localStorage.setItem('medicamentsPersonnalises', JSON.stringify(medsPersonnalises));

                    showMessage(`${nbAjoutes} médicament(s) importé(s) avec succès !`, "green");
                    importMedsInput.value = '';
                } catch (error) {
                    showMessage("Erreur lors de l'import : " + error.message, "red");
                }
            };
            reader.readAsText(file);
        });
    }

    // === Chargement initial ===
    try {
        // Test de localStorage
        console.log('📌 Test de localStorage dans options.html');
        const testValue = localStorage.getItem('test_ord_html');
        console.log('✅ Test lu depuis localStorage:', testValue);
        localStorage.setItem('test_options_html', new Date().toISOString());
        console.log('✅ Test écrit dans localStorage, clé: test_options_html');
        
        chargerOrdonnancesTypes();
        showMessage("Données chargées avec succès.", "green");
    } catch (error) {
        showMessage("Erreur lors du chargement des données.", "red");
        console.error("Erreur :", error);
    }
});

// === Fonctions ===

// Charger les ordonnances types
function chargerOrdonnancesTypes() {
    let data = JSON.parse(localStorage.getItem("ordonnancesTypesPourOrd") || '{}');
    remplirListeOrdonnancesTypes(data);
}

// Remplir la liste HTML des ordonnances types
function remplirListeOrdonnancesTypes(data) {
    const container = document.getElementById("ordonnances-liste");
    container.innerHTML = "";

    Object.keys(data).forEach(nom => {
        const ordonnanceItem = document.createElement("div");
        ordonnanceItem.className = "ordonnance-item";
        ordonnanceItem.innerHTML = `
            <strong>${nom}</strong>
            <div class="ordonnance-medicaments">
                ${data[nom].map(med => `
                    <div class="ordonnance-medicament">
                        <span><strong>Médicament:</strong> ${med.medicament}</span>
                        <span><strong>Posologie:</strong> ${med.posologie}</span>
                        <span><strong>Quantité:</strong> ${med.quantite}</span>
                    </div>
                `).join('')}
            </div>
            <div class="ordonnance-actions">
                <button class="modifier-ordonnance btn-secondary" data-nom="${nom}">Modifier</button>
                <button class="supprimer-ordonnance btn-danger" data-nom="${nom}">Supprimer</button>
            </div>
        `;
        container.appendChild(ordonnanceItem);
    });

    // Ajouter les événements pour les boutons de modification et suppression
    document.querySelectorAll('.modifier-ordonnance').forEach(btn => {
        btn.addEventListener('click', function() {
            const nom = this.getAttribute('data-nom');
            modifierOrdonnance(nom);
        });
    });

    document.querySelectorAll('.supprimer-ordonnance').forEach(btn => {
        btn.addEventListener('click', function() {
            const nom = this.getAttribute('data-nom');
            supprimerOrdonnanceDirecte(nom);
        });
    });

    console.log("Liste des ordonnances types mise à jour :", data);
}

// Fonction pour modifier une ordonnance existante
function modifierOrdonnance(nom) {
    let data = JSON.parse(localStorage.getItem("ordonnancesTypesPourOrd") || '{}');
    const ordonnance = data[nom];

    if (ordonnance) {
            // Afficher le formulaire
            document.getElementById("formulaire-ordonnance").classList.remove("hidden");
            document.getElementById("titre-formulaire").textContent = `Modifier l'ordonnance: ${nom}`;
            document.getElementById("nom-ordonnance").value = nom;
            document.getElementById("supprimer-ordonnance").classList.remove("hidden");
            document.getElementById("supprimer-ordonnance").setAttribute("data-nom", nom);

            // Remplir la liste des médicaments
            const listeMeds = document.getElementById("liste-medicaments-ordonnance");
            listeMeds.innerHTML = "";

            ordonnance.forEach((med, index) => {
                const medDiv = document.createElement("div");
                medDiv.className = "form-row";
                medDiv.innerHTML = `
                    <div class="form-group">
                        <label>Médicament</label>
                        <input type="text" class="medicament-input" value="${med.medicament}" placeholder="Nom du médicament">
                    </div>
                    <div class="form-group">
                        <label>Posologie</label>
                        <input type="text" class="posologie-input" value="${med.posologie}" placeholder="Posologie">
                    </div>
                    <div class="form-group">
                        <label>Quantité</label>
                        <input type="text" class="quantite-input" value="${med.quantite}" placeholder="Quantité">
                    </div>
                    <div class="form-group" style="display: flex; align-items: flex-end;">
                        <button type="button" class="btn-danger supprimer-medicament" data-index="${index}">Supprimer</button>
                    </div>
                `;
                listeMeds.appendChild(medDiv);
            });

            // Ajouter les événements pour supprimer des médicaments
            document.querySelectorAll('.supprimer-medicament').forEach(btn => {
                btn.addEventListener('click', function() {
                    const index = parseInt(this.getAttribute('data-index'));
                    const medicamentDiv = this.closest('.form-row');
                    medicamentDiv.remove();
                });
            });

            // Faire défiler vers le formulaire
            document.getElementById("formulaire-ordonnance").scrollIntoView({ behavior: "smooth" });
        }
}

// Fonction pour ajouter un médicament dans le formulaire
function ajouterMedicamentForm() {
    const listeMeds = document.getElementById("liste-medicaments-ordonnance");
    const index = listeMeds.children.length;
    const medDiv = document.createElement("div");
    medDiv.className = "form-row";
    medDiv.innerHTML = `
        <div class="form-group">
            <label>Médicament</label>
            <input type="text" class="medicament-input" placeholder="Nom du médicament">
        </div>
        <div class="form-group">
            <label>Posologie</label>
            <input type="text" class="posologie-input" placeholder="Posologie">
        </div>
        <div class="form-group">
            <label>Quantité</label>
            <input type="text" class="quantite-input" placeholder="Quantité">
        </div>
        <div class="form-group" style="display: flex; align-items: flex-end;">
            <button type="button" class="btn-danger supprimer-medicament" data-index="${index}">Supprimer</button>
        </div>
    `;
    listeMeds.appendChild(medDiv);

    // Ajouter l'événement pour supprimer ce médicament
    const supprimerBtn = medDiv.querySelector('.supprimer-medicament');
    supprimerBtn.addEventListener('click', function() {
        const medicamentDiv = this.closest('.form-row');
        medicamentDiv.remove();
    });
}

// Fonction pour enregistrer une ordonnance (ajouter ou modifier)
function enregistrerOrdonnance() {
    const nom = document.getElementById("nom-ordonnance").value.trim();
    if (!nom) {
        showMessage("Veuillez saisir un nom pour l'ordonnance.", "red");
        return;
    }

    // Récupérer tous les médicaments du formulaire
    const medicamentInputs = document.querySelectorAll('.medicament-input');
    const posologieInputs = document.querySelectorAll('.posologie-input');
    const quantiteInputs = document.querySelectorAll('.quantite-input');

    if (medicamentInputs.length === 0) {
        showMessage("Veuillez ajouter au moins un médicament.", "red");
        return;
    }

    const medicaments = [];
    for (let i = 0; i < medicamentInputs.length; i++) {
        const medicament = medicamentInputs[i].value.trim();
        const posologie = posologieInputs[i].value.trim();
        const quantite = quantiteInputs[i].value.trim();

        if (!medicament || !posologie || !quantite) {
            showMessage(`Veuillez remplir tous les champs pour le médicament ${i + 1}.`, "red");
            return;
        }

        medicaments.push({
            medicament: medicament,
            posologie: posologie,
            quantite: quantite
        });
    }

    // Sauvegarder l'ordonnance
    let data = JSON.parse(localStorage.getItem("ordonnancesTypesPourOrd") || '{}');
    const ancienNom = document.getElementById("nom-ordonnance").value;

    // Supprimer l'ancienne entrée si c'est une modification
    if (document.getElementById("titre-formulaire").textContent.includes("Modifier")) {
        delete data[ancienNom];
    }

    // Ajouter la nouvelle/la mise à jour
    data[nom] = medicaments;

    localStorage.setItem("ordonnancesTypes", JSON.stringify(data));
    localStorage.setItem("ordonnancesTypesPourOrd", JSON.stringify(data));
    chargerOrdonnancesTypes();
    annulerOrdonnance();
    showMessage(`Ordonnance "${nom}" enregistrée avec succès !`, "green");
}

// Fonction pour annuler la modification/ajout d'une ordonnance
function annulerOrdonnance() {
    document.getElementById("formulaire-ordonnance").classList.add("hidden");
    document.getElementById("titre-formulaire").textContent = "Ajouter une nouvelle ordonnance type";
    document.getElementById("nom-ordonnance").value = "";
    document.getElementById("liste-medicaments-ordonnance").innerHTML = "";
    document.getElementById("supprimer-ordonnance").classList.add("hidden");
}

// Fonction pour supprimer une ordonnance (appelée depuis le bouton de suppression)
function supprimerOrdonnance() {
    const nom = document.getElementById("supprimer-ordonnance").getAttribute("data-nom");
    if (confirm(`Êtes-vous sûr de vouloir supprimer l'ordonnance "${nom}" ?`)) {
        supprimerOrdonnanceDirecte(nom);
    }
}

// Fonction pour supprimer une ordonnance directement
function supprimerOrdonnanceDirecte(nom) {
    let data = JSON.parse(localStorage.getItem("ordonnancesTypesPourOrd") || '{}');
    delete data[nom];

    localStorage.setItem("ordonnancesTypes", JSON.stringify(data));
    localStorage.setItem("ordonnancesTypesPourOrd", JSON.stringify(data));
    chargerOrdonnancesTypes();
    showMessage(`Ordonnance "${nom}" supprimée avec succès !`, "green");
}

// Afficher un message à l'utilisateur
function showMessage(message, color) {
    const msg = document.getElementById("message");
    msg.textContent = message;
    msg.style.color = color;
}
