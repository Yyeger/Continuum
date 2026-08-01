doc_root = Path.expand("../doc", __DIR__)

failures =
  doc_root
  |> Path.join("**/*.html")
  |> Path.wildcard()
  |> Enum.flat_map(fn page ->
    page
    |> File.read!()
    |> then(&Regex.scan(~r/(?:href|src)="([^"]+)"/, &1, capture: :all_but_first))
    |> Enum.map(&hd/1)
    |> Enum.reject(fn target ->
      String.starts_with?(target, ["#", "/", "http://", "https://", "mailto:", "data:"])
    end)
    |> Enum.reject(&(&1 == "docs_config.js"))
    |> Enum.reject(fn target ->
      target = target |> String.split(["#", "?"], parts: 2) |> hd() |> URI.decode()
      path = Path.expand(target, Path.dirname(page))
      File.exists?(path) or File.exists?(Path.join(path, "index.html"))
    end)
    |> Enum.map(&{Path.relative_to(page, doc_root), &1})
  end)

case failures do
  [] ->
    IO.puts("Generated documentation links are valid")

  failures ->
    Enum.each(failures, fn {page, target} -> IO.puts(:stderr, "#{page}: missing #{target}") end)
    System.halt(1)
end
