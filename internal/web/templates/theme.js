// Applies the saved theme before first paint (this script loads in <head>)
// and wires up the picker. "system" leaves data-theme unset so the
// stylesheet follows prefers-color-scheme.
(function () {
  var KEY = "gitea-mq-theme";
  var root = document.documentElement;

  function saved() {
    try { return localStorage.getItem(KEY) || "system"; } catch (e) { return "system"; }
  }

  function apply(theme) {
    if (theme === "light" || theme === "dark") root.dataset.theme = theme;
    else delete root.dataset.theme;
  }

  apply(saved());

  document.addEventListener("DOMContentLoaded", function () {
    var select = document.getElementById("theme");
    if (!select) return;
    select.value = saved();
    select.addEventListener("change", function () {
      try { localStorage.setItem(KEY, select.value); } catch (e) { /* private mode */ }
      apply(select.value);
    });
    // The picker is hidden in the markup so it never shows without JS.
    select.parentElement.hidden = false;
  });
})();
