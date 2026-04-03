const releaseStatus = document.getElementById("release-status");

async function loadLatestRelease() {
  try {
    const response = await fetch("https://api.github.com/repos/khangpt2k6/GoQueue/releases/latest", {
      headers: { Accept: "application/vnd.github+json" },
    });

    if (!response.ok) {
      throw new Error(`GitHub API returned ${response.status}`);
    }

    const release = await response.json();
    const publishedAt = release.published_at
      ? new Date(release.published_at).toLocaleDateString()
      : "unknown date";

    releaseStatus.innerHTML = `Latest release: <strong>${release.tag_name || "N/A"}</strong> (${publishedAt}) - <a href="${release.html_url}" target="_blank" rel="noreferrer">view notes</a>`;
  } catch (error) {
    releaseStatus.textContent = "Latest release unavailable right now.";
  }
}

loadLatestRelease();
