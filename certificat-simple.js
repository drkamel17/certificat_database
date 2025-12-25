// Script des fonctions de certificat - version simple et fonctionnelle

// Fonction pour sauvegarder les modifications des champs
function sauvegarderModifications() {
    const textInputs = document.querySelectorAll('.full-width-input');
    textInputs.forEach(input => {
        input.addEventListener('input', () => {
            input.setAttribute('value', input.value);
        });
    });
}

// Fonction pour charger les données depuis localStorage
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
}

// Fonctions pour générer les certificats
function genererCertificat() {
    console.log('Génération du certificat d\'éviction scolaire...');
}

function genererArretTravail() {
    console.log('Génération du certificat d\'arrêt de travail...');
}

function genererProlongation() {
    console.log('Génération du certificat de prolongation...');
}

function genererLettre() {
    console.log('Génération de la lettre médicale...');
}

function genererRadiox() {
    console.log('Génération du certificat radiox...');
}

function genererChronique() {
    console.log('Génération du certificat de maladie chronique...');
}

function genererNonGrossesse() {
    console.log('Génération du certificat de non-grossesse...');
}

function genererReprise() {
    console.log('Génération du certificat de reprise de travail...');
}

function genererCvb() {
    console.log('Génération du certificat CBV...');
    alert('CBV - Fonction en cours de développement. API Python requis.');
}

// Initialisation
document.addEventListener('DOMContentLoaded', () => {
    console.log('Chargement des fonctions de certificat...');
    loadData();
    sauvegarderModifications();
});