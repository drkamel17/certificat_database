﻿// popup.js
// Écouter les messages pour ouvrir popup.js
// Note: Chrome extension APIs have been replaced with standard web APIs
// In a web context, this would be handled differently or removed


// Declaration de la variable pour stocker les medicaments
const ordonnanceMedicaments = [];

// Fonction pour capitaliser automatiquement les noms et prénoms
function capitalizeNames(text) {
    if (!text) return text;
    return text.toLowerCase().replace(/\b\w/g, letter => letter.toUpperCase());
}

document.addEventListener("DOMContentLoaded", async () => {
    // Gerer l'ouverture de la modale
    const gererMedicamentsBtn = document.getElementById("gerer-medicaments");
    if (gererMedicamentsBtn) {
        gererMedicamentsBtn.addEventListener("click", () => {
            afficherMedicamentsPersonnalises();
            document.getElementById("modal").style.display = "block";
        });
    }

    // Gerer la fermeture de la modale
    const closeBtn = document.querySelector(".close");
    if (closeBtn) {
        closeBtn.onclick = function () {
            document.getElementById("modal").style.display = "none";
        };
    }

    window.onclick = function (event) {
        if (event.target == document.getElementById("modal")) {
            document.getElementById("modal").style.display = "none";
        }
    };

    // Vider les medicaments
    const viderMedicamentsBtn = document.getElementById("vider-medicaments");
    if (viderMedicamentsBtn) {
        viderMedicamentsBtn.addEventListener("click", () => {
            if (confirm("Voulez-vous vraiment supprimer tous les medicaments enregistres ?")) {
                localStorage.setItem("medicaments", JSON.stringify([]));
                afficherMedicamentsPersonnalises();
                remplirDatalist([]);
            }
        });
    }

    // Gestion de la navigation au clavier entre les champs
    const champMedicament = document.getElementById("medicament");
    const champDosage = document.getElementById("dosage");
    const champDuree = document.getElementById("duree");
    const champFrequence = document.getElementById("frequence");
    const champQuantite = document.getElementById("quantite");
    const champPosologie = document.getElementById("posologie");

    if (champMedicament && champDosage) {
        champMedicament.addEventListener("keydown", (e) => {
            if (e.key === "Enter") {
                e.preventDefault();
                champDosage.focus();
            }
        });

        champDosage.addEventListener("keydown", (e) => {
            if (e.key === "Enter") {
                e.preventDefault();
                champDuree?.focus();
            }
        });
    }

    if (champDuree && champFrequence) {
        champDuree.addEventListener("keydown", (e) => {
            if (e.key === "Enter") {
                e.preventDefault();
                champFrequence.focus();
            }
        });
    }

    if (champFrequence && champQuantite) {
        champFrequence.addEventListener("keydown", (e) => {
            if (e.key === "Enter") {
                e.preventDefault();
                champQuantite.focus();
            }
        });
    }

    if (champQuantite && champPosologie) {
        champQuantite.addEventListener("keydown", (e) => {
            if (e.key === "Enter") {
                e.preventDefault();
                champPosologie.focus();
            }
        });
    }

    // Charger les données de stockage
    const data = JSON.parse(localStorage.getItem('patientData') || '{}');
    const nomInput = document.getElementById("nom");
    const prenomInput = document.getElementById("prenom");
    const dobInput = document.getElementById("dob");
    const poidsInput = document.getElementById("poids");

    if (nomInput) nomInput.value = data.nom || "";
    if (prenomInput) prenomInput.value = data.prenom || "";
    if (dobInput) dobInput.value = data.dob || "";
    if (poidsInput) poidsInput.value = data.poids || "";

    // Charger les médicaments
    try {
        let meds = await chargerMedicaments();
        remplirDatalist(meds);
    } catch (error) {
        console.error("Erreur lors du chargement des médicaments:", error);
    }

    // Ajouter un médicament à l'ordonnance
    const ajouterMedicamentBtn = document.getElementById("ajouter-medicament");
    if (ajouterMedicamentBtn) {
        ajouterMedicamentBtn.addEventListener("click", () => {
            const medicament = document.getElementById("medicament")?.value.trim();
            const dosage = document.getElementById("dosage")?.value.trim();
            const duree = document.getElementById("duree")?.value.trim();
            const frequence = document.getElementById("frequence")?.value.trim();
            const quantite = document.getElementById("quantite")?.value.trim();
            const posologie = document.getElementById("posologie")?.value.trim();

            if (!medicament) {
                alert("Veuillez entrer un médicament");
                return;
            }

            const medicamentObj = {
                medicament,
                dosage,
                duree,
                frequence,
                quantite,
                posologie
            };

            ordonnanceMedicaments.push(medicamentObj);
            afficherMedicamentsPersonnalises();

            // Réinitialiser les champs
            if (document.getElementById("medicament")) document.getElementById("medicament").value = "";
            if (document.getElementById("dosage")) document.getElementById("dosage").value = "";
            if (document.getElementById("duree")) document.getElementById("duree").value = "";
            if (document.getElementById("frequence")) document.getElementById("frequence").value = "";
            if (document.getElementById("quantite")) document.getElementById("quantite").value = "";
            if (document.getElementById("posologie")) document.getElementById("posologie").value = "";

            // Mettre le focus sur le premier champ
            if (document.getElementById("medicament")) document.getElementById("medicament").focus();
        });
    }

    // Générer l'ordonnance
    const genererOrdonnanceBtn = document.getElementById("generer-ordonnance");
    if (genererOrdonnanceBtn) {
        genererOrdonnanceBtn.addEventListener("click", async () => {
            const nom = document.getElementById("nom")?.value.trim();
            const prenom = document.getElementById("prenom")?.value.trim();
            const dob = document.getElementById("dob")?.value.trim();
            const poids = document.getElementById("poids")?.value.trim();

            if (!nom || !prenom || !dob || !poids) {
                alert("Veuillez remplir tous les champs du patient");
                return;
            }

            if (ordonnanceMedicaments.length === 0) {
                alert("Veuillez ajouter au moins un médicament");
                return;
            }

            // Stocker les données du patient
            const patientData = { nom, prenom, dob, poids };
            localStorage.setItem('patientData', JSON.stringify(patientData));

            // Stocker les médicaments
            localStorage.setItem('medicaments', JSON.stringify(ordonnanceMedicaments));

            // Ouvrir la page d'ordonnance
            window.open('ord.html', '_blank');
        });
    }
});

// Charger les médicaments depuis le stockage ou le fichier local
async function chargerMedicaments() {
    const meds = JSON.parse(localStorage.getItem("medicaments") || "[]");
    return meds;
}

// Remplir le datalist avec les médicaments
function remplirDatalist(medicaments) {
    const datalist = document.getElementById("medicaments-list");
    if (datalist) {
        datalist.innerHTML = "";

        medicaments.forEach(med => {
            const option = document.createElement("option");
            option.value = med;
            datalist.appendChild(option);
        });
    }
}

// Afficher les médicaments ajoutés
function afficherMedicamentsPersonnalises() {
    const liste = document.getElementById("medicaments-personnalises");
    if (!liste) return;

    liste.innerHTML = "";

    if (ordonnanceMedicaments.length === 0) {
        liste.innerHTML = "<li>Aucun médicament ajouté</li>";
        return;
    }

    ordonnanceMedicaments.forEach((med, index) => {
        const li = document.createElement("li");
        li.textContent = `${med.medicament} - ${med.dosage} - ${med.duree} - ${med.frequence} - ${med.quantite} - ${med.posologie}`;
        li.innerHTML += `<button class="supprimer" data-index="${index}">Supprimer</button>`;
        liste.appendChild(li);
    });

    // Ajouter les écouteurs pour les boutons de suppression
    document.querySelectorAll(".supprimer").forEach(btn => {
        btn.addEventListener("click", (e) => {
            const index = parseInt(e.target.getAttribute("data-index"));
            ordonnanceMedicaments.splice(index, 1);
            afficherMedicamentsPersonnalises();
        });
    });
}