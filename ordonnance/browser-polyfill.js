// Browser API polyfill for Chrome/Edge compatibility
if (typeof browser === 'undefined') {
    // Check if we're in a Chrome extension environment
    if (typeof chrome !== 'undefined' && chrome.runtime) {
        window.browser = chrome;
    } else {
        // Create a mock browser API for web environments
        window.browser = {
            storage: {
                local: {
                    get: function(keys) {
                        return new Promise((resolve) => {
                            if (typeof keys === 'string') {
                                keys = [keys];
                            }
                            const result = {};
                            keys.forEach(key => {
                                const value = localStorage.getItem(key);
                                result[key] = value ? JSON.parse(value) : undefined;
                            });
                            resolve(result);
                        });
                    },
                    set: function(items) {
                        return new Promise((resolve) => {
                            Object.keys(items).forEach(key => {
                                localStorage.setItem(key, JSON.stringify(items[key]));
                            });
                            resolve();
                        });
                    }
                }
            },
            runtime: {
                getURL: function(path) {
                    // Return the path as-is for web environment
                    return path;
                }
            }
        };
    }
}
