// Browser API polyfill for Chrome/Edge compatibility
if (typeof browser === 'undefined') {
    window.browser = chrome;
}