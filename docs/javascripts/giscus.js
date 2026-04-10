const GISCUS_CONFIG = {
  repo: "agent-jaden/order-backlog-dashboard",
  repoId: "R_kgDOR-nCXg",
  category: "Announcements",
  categoryId: "DIC_kwDOR-nCXs4C6h49",
  mapping: "pathname",
  strict: "0",
  reactionsEnabled: "1",
  emitMetadata: "0",
  inputPosition: "bottom",
  lang: "ko",
  loading: "lazy",
};

function isConfigured(config) {
  return (
    config.repo &&
    config.repoId &&
    config.category &&
    config.categoryId &&
    !config.repoId.startsWith("REPLACE_WITH_") &&
    !config.categoryId.startsWith("REPLACE_WITH_")
  );
}

function createMessage() {
  const wrapper = document.createElement("section");
  wrapper.className = "giscus-comments giscus-comments--inactive";
  wrapper.innerHTML = `
    <h2>댓글</h2>
    <p>
      giscus 설정이 아직 완료되지 않았습니다.
      <code>docs/javascripts/giscus.js</code>에서 <code>repoId</code>와
      <code>categoryId</code>를 채우면 댓글이 활성화됩니다.
    </p>
  `;
  return wrapper;
}

function createComments(config) {
  const wrapper = document.createElement("section");
  wrapper.className = "giscus-comments";

  const heading = document.createElement("h2");
  heading.textContent = "댓글";
  wrapper.appendChild(heading);

  const script = document.createElement("script");
  script.src = "https://giscus.app/client.js";
  script.async = true;
  script.crossOrigin = "anonymous";
  script.setAttribute("data-repo", config.repo);
  script.setAttribute("data-repo-id", config.repoId);
  script.setAttribute("data-category", config.category);
  script.setAttribute("data-category-id", config.categoryId);
  script.setAttribute("data-mapping", config.mapping);
  script.setAttribute("data-strict", config.strict);
  script.setAttribute("data-reactions-enabled", config.reactionsEnabled);
  script.setAttribute("data-emit-metadata", config.emitMetadata);
  script.setAttribute("data-input-position", config.inputPosition);
  script.setAttribute("data-theme", "preferred_color_scheme");
  script.setAttribute("data-lang", config.lang);
  script.setAttribute("data-loading", config.loading);

  wrapper.appendChild(script);
  return wrapper;
}

function mountGiscus() {
  if (document.querySelector(".giscus-comments")) {
    return;
  }

  const article = document.querySelector("main .md-content__inner");
  if (!article) {
    return;
  }

  const comments = isConfigured(GISCUS_CONFIG)
    ? createComments(GISCUS_CONFIG)
    : createMessage();

  article.appendChild(comments);
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", mountGiscus);
} else {
  mountGiscus();
}
