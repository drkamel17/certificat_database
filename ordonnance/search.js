// Gestion de la recherche dynamique sous input ou textarea
function initializeSearch() {
    console.log('Initialisation de la recherche');
    console.log('Statut de connexion:', navigator.onLine);

    // Sélectionner tous les champs de recherche
    const searchInputs = [
        document.getElementById('searchInput'),
        document.getElementById('searchInput2'),
        document.getElementById('searchInput3'),
        document.getElementById('searchInput4')
    ].filter(Boolean); // Filtrer les éléments null

    const textareas = document.querySelectorAll('textarea');
    const lockButton = document.getElementById('lockButton');
    const viewSavedButton = document.getElementById('viewSavedButton');
    const transferButton = document.getElementById('transferButton');
    const importButton = document.getElementById('importButton');

    const suggestionsDiv = document.createElement('div');
    suggestionsDiv.id = 'suggestions';
    suggestionsDiv.style.position = 'absolute';
    suggestionsDiv.style.zIndex = '9999';
    suggestionsDiv.style.backgroundColor = 'white';
    suggestionsDiv.style.border = '1px solid #ccc';
    suggestionsDiv.style.maxHeight = '200px';
    suggestionsDiv.style.overflowY = 'auto';
    suggestionsDiv.style.display = 'none';
    document.body.appendChild(suggestionsDiv);

    const messageBox = document.createElement('div');
    messageBox.className = 'message-box';
    document.body.appendChild(messageBox);

    let currentTarget = null;
    let isOnline = navigator.onLine;

    // Récupérer et enregistrer les termes
    function loadMedicalTerms() {
        return new Promise((resolve) => {
            browser.storage.local.get('medicalTerms', function(result) {
                resolve(result.medicalTerms || []);
            });
        });
    }

    // Sauvegarder les termes
    function saveMedicalTerms(terms) {
        return new Promise((resolve) => {
            browser.storage.local.set({ medicalTerms: terms }, function() {
                resolve(true);
            });
        });
    }

    // Fonction pour sauvegarder automatiquement les nouveaux termes
    function autoSaveNewTerm(inputValue) {
        if (!inputValue || typeof inputValue !== 'string') return;
        
        const words = inputValue.trim().split(/\s+/);
        
        if (words.length >= 1 && inputValue.endsWith(' ')) {
            // Prendre le dernier mot avant l'espace
            const completedWord = words[words.length - 1].trim();

            console.log("Tentative d'enregistrement automatique :", completedWord);

            if (completedWord && completedWord.length > 1) {
                loadMedicalTerms().then(terms => {
                    if (!terms.includes(completedWord)) {
                        terms.push(completedWord);
                        saveMedicalTerms(terms).then(success => {
                            if (success) {
                                console.log('Terme sauvegardé automatiquement :', completedWord);
                                
                                // Créer un message de notification
                                const notification = document.createElement('div');
                                notification.textContent = 'Nouveau terme sauvegardé : ' + completedWord;
                                notification.style.position = 'fixed';
                                notification.style.bottom = '20px';
                                notification.style.right = '20px';
                                notification.style.backgroundColor = '#4CAF50';
                                notification.style.color = 'white';
                                notification.style.padding = '10px 20px';
                                notification.style.borderRadius = '4px';
                                notification.style.zIndex = '10000';
                                notification.style.boxShadow = '0 2px 10px rgba(0,0,0,0.2)';
                                notification.style.animation = 'fadeIn 0.3s';
                                
                                // Style pour l'animation
                                const style = document.createElement('style');
                                style.textContent = `
                                    @keyframes fadeIn {
                                        from { opacity: 0; transform: translateY(20px); }
                                        to { opacity: 1; transform: translateY(0); }
                                    }
                                    @keyframes fadeOut {
                                        from { opacity: 1; transform: translateY(0); }
                                        to { opacity: 0; transform: translateY(20px); }
                                    }
                                `;
                                document.head.appendChild(style);
                                
                                document.body.appendChild(notification);
                                
                                // Supprimer la notification après 2 secondes
                                setTimeout(() => {
                                    if (document.body.contains(notification)) {
                                        notification.style.animation = 'fadeOut 0.3s';
                                        setTimeout(() => {
                                            if (document.body.contains(notification)) {
                                                document.body.removeChild(notification);
                                            }
                                        }, 300);
                                    }
                                }, 2000);
                            }
                        }).catch(error => {
                            console.error("Erreur lors de la sauvegarde du terme :", error);
                        });
                    }
                }).catch(error => {
                    console.error("Erreur lors du chargement des termes :", error);
                });
            }
        }
    }
    
    // Exposer la fonction globalement
    window.autoSaveNewTerm = autoSaveNewTerm;


    // Ajout d'un terme
    if (lockButton) {
        lockButton.addEventListener('click', function() {
            const term = (currentTarget?.value || '').trim();
            if (term) {
                loadMedicalTerms().then(terms => {
                    if (!terms.includes(term)) {
                        terms.push(term);
                        saveMedicalTerms(terms).then(success => {
                            if (success) {
                                messageBox.innerHTML = 'Terme sauvegardé avec succès !';
                                messageBox.style.color = 'green';
                                setTimeout(() => { messageBox.innerHTML = ''; }, 3000);
                            }
                        });
                    }
                });
            }
        });
    }

    // Afficher les termes sauvegardés
    if (viewSavedButton) {
        let popupWindow = null;
        viewSavedButton.addEventListener('click', function() {
            if (popupWindow && !popupWindow.closed) {
                popupWindow.close();
                popupWindow = null;
                return;
            }
            
            popupWindow = window.open(
                chrome.runtime.getURL('templates/popup-terms.html'),
                '_blank',
                'width=400,height=600,left=100,top=100'
            );
            
            const cleanUp = () => {
                try {
                    popupWindow.close();
                } catch(e) {}
                popupWindow = null;
            };

            window.addEventListener('beforeunload', cleanUp);
            
            popupWindow.onload = function() {
                loadMedicalTerms().then(terms => {
                    if (!popupWindow || popupWindow.closed) return;
                    
                    if (terms.length) {
                        const termsList = popupWindow.document.getElementById('termsList');
                        termsList.innerHTML = terms.map(term => `<li>${term}</li>`).join('');
                    } else {
                        alert('Aucun terme sauvegardé');
                        cleanUp();
                    }
                });
            };
        });
    }

    // Exporter les termes
    if (transferButton) {
        transferButton.addEventListener('click', function() {
            loadMedicalTerms().then(terms => {
                const blob = new Blob([JSON.stringify(terms, null, 2)], { type: 'application/json' });
                const url = window.URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = 'medical_terms.json';
                document.body.appendChild(a);
                a.click();
                document.body.removeChild(a);
                window.URL.revokeObjectURL(url);
            });
        });
    }

    // Importer les termes
    if (importButton) {
        importButton.addEventListener('click', function() {
            const input = document.createElement('input');
            input.type = 'file';
            input.accept = '.json';
            input.onchange = function(e) {
                const file = e.target.files[0];
                if (file) {
                    const reader = new FileReader();
                    reader.onload = function(e) {
                        try {
                            const terms = JSON.parse(e.target.result);
                            saveMedicalTerms(terms).then(success => {
                                if (success) {
                                    // Afficher le message dans la boîte de message
                                    messageBox.innerHTML = 'Termes importés avec succès !';
                                    messageBox.style.color = 'green';
                                    
                                    // Afficher une alerte
                                    alert('Les termes médicaux ont été importés avec succès !');
                                    
                                    // Afficher une notification stylisée
                                    const notification = document.createElement('div');
                                    notification.textContent = 'Importation réussie !';
                                    notification.style.position = 'fixed';
                                    notification.style.top = '20px';
                                    notification.style.right = '20px';
                                    notification.style.backgroundColor = '#4CAF50';
                                    notification.style.color = 'white';
                                    notification.style.padding = '15px 25px';
                                    notification.style.borderRadius = '4px';
                                    notification.style.zIndex = '10000';
                                    notification.style.boxShadow = '0 2px 10px rgba(0,0,0,0.2)';
                                    notification.style.animation = 'fadeIn 0.3s';
                                    document.body.appendChild(notification);
                                    
                                    // Supprimer la notification après 3 secondes
                                    setTimeout(() => {
                                        notification.style.animation = 'fadeOut 0.3s';
                                        setTimeout(() => {
                                            if (document.body.contains(notification)) {
                                                document.body.removeChild(notification);
                                            }
                                        }, 300);
                                    }, 3000);
                                }
                            });
                        } catch (error) {
                            messageBox.innerHTML = 'Erreur d\'import : fichier invalide';
                            messageBox.style.color = 'red';
                        }
                        setTimeout(() => { messageBox.innerHTML = ''; }, 3000);
                    };
                    reader.readAsText(file);
                }
            };
            input.click();
        });
    }

    // Suggestions locales
    function showLocalSuggestions(searchTerm) {
        loadMedicalTerms().then(terms => {
            const filtered = terms.filter(t => t.toLowerCase().includes(searchTerm.toLowerCase())).slice(0, 10);
            suggestionsDiv.innerHTML = filtered.map(t =>
                `<div class="suggestion-item" data-term="${t}">${t}</div>`
            ).join('');
            if (filtered.length > 0) {
                positionSuggestions(currentTarget);
                suggestionsDiv.style.display = 'block';
                addClickHandlers();
            } else {
                suggestionsDiv.style.display = 'none';
            }
        });
    }

    // Positionner les suggestions sous le champ actif
    function positionSuggestions(input) {
        if (!input) return;
        const rect = input.getBoundingClientRect();
        suggestionsDiv.style.top = `${rect.bottom + window.scrollY}px`;
        suggestionsDiv.style.left = `${rect.left + window.scrollX}px`;
        suggestionsDiv.style.width = `${rect.width}px`;
    }

    // Gérer clic sur suggestion
    function addClickHandlers() {
        document.querySelectorAll('.suggestion-item').forEach(item => {
            item.addEventListener('click', function() {
                const term = this.getAttribute('data-term');
                if (currentTarget) currentTarget.value = term;
                suggestionsDiv.style.display = 'none';
            });
        });
    }

    // Traitement commun pour tout champ (input/textarea)
function attachInputHandler(field) {
    field.addEventListener('focus', () => {
        currentTarget = field;
    });

    field.addEventListener('input', () => {
        const value = field.value.trim();
        
        // Sauvegarder le mot avant l'espace
        autoSaveNewTerm(value);

        // Afficher les suggestions
        const words = value.split(' ');
        const lastWord = words[words.length - 1];

        if (!lastWord) {
            suggestionsDiv.style.display = 'none';
            return;
        }

        // Toujours essayer d'afficher les suggestions locales d'abord
        showLocalSuggestions(lastWord);
        
        // Si en ligne et le mot fait plus d'un caractère, essayer d'obtenir des suggestions en ligne
        if (isOnline && lastWord.length > 1) {
            // Utiliser une requête avec un timeout
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 2000); // Timeout après 2 secondes
            
            fetch(`https://api.datamuse.com/sug?s=${encodeURIComponent(lastWord)}`, {
                signal: controller.signal
            })
            .then(res => {
                clearTimeout(timeoutId);
                if (!res.ok) throw new Error('Network response was not ok');
                return res.json();
            })
            .then(data => {
                if (data && Array.isArray(data) && data.length > 0) {
                    const html = data.map(item => {
                        const fullTerm = [...words.slice(0, -1), item.word].join(' ');
                        return `<div class="suggestion-item" data-term="${fullTerm}">${item.word}</div>`;
                    }).join('');
                    suggestionsDiv.innerHTML = html;
                    positionSuggestions(field);
                    suggestionsDiv.style.display = 'block';
                    addClickHandlers();
                }
            })
            .catch(error => {
                if (error.name !== 'AbortError') {
                    console.log('Erreur lors du chargement des suggestions :', error.message);
                }
                // On garde les suggestions locales déjà affichées
            });
        }
    });
}


    // Appliquer à tous les champs de recherche
    searchInputs.forEach(input => attachInputHandler(input));
    textareas.forEach(ta => attachInputHandler(ta));

    // Gérer changement d'état réseau
    window.addEventListener('online', () => { isOnline = true; });
    window.addEventListener('offline', () => { isOnline = false; });
	
	    // Masquer suggestions si clic en dehors
    document.addEventListener('click', function (e) {
        if (
            !suggestionsDiv.contains(e.target) &&
            currentTarget !== e.target
        ) {
            suggestionsDiv.style.display = 'none';
        }
    });
	    // Masquer suggestions si on appuie sur Échap
    document.addEventListener('keydown', function (e) {
        if (e.key === 'Escape' || e.key === 'Enter') {
            suggestionsDiv.style.display = 'none';
        }
    });


}

// Initialiser la recherche lorsque le DOM est chargé
document.addEventListener('DOMContentLoaded', function() {
    try {
        initializeSearch();
        console.log('Recherche initialisée avec succès');
    } catch (error) {
        console.error('Erreur lors de l\'initialisation de la recherche :', error);
    }
});

// S'assurer que la fonction est disponible même si le DOM est déjà chargé
if (document.readyState === 'complete' || document.readyState === 'interactive') {
    setTimeout(initializeSearch, 1);
}
