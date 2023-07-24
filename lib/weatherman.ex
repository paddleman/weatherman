defmodule Weatherman do

  alias Weatherman.Constants

  def start_swob_download(date) do
    base_url = "https://dd.weather.gc.ca/observations/swob-ml/"
    dest_base = "/home/dj/data/datamart/swob_ml/"

    # stns = Constants.bc_swob_stations
    stns = Constants.new_bc_stns()
    # stns = Constants.special_bc_stns

    avail_stns = get_stn_list_for_date(date, base_url)

    final_stns = stns
                  |> MapSet.new
                  |> MapSet.intersection(avail_stns)
                  |> MapSet.to_list()
                  |> Enum.sort()
                  |> IO.inspect()

    final_stns
    |> Enum.map(fn x -> File.mkdir_p!(dest_base <> x <> "/" <> date <> "/") end)

    final_stns
    |> Enum.map(fn stn -> develop_file_list_and_download(stn, date, base_url, dest_base) end)

  end


  def get_stn_list_for_date(date, base_url) do
    url = base_url <> date <> "/"
    resp = HTTPoison.get!(url, [], [timeout: 160_000, recv_timeout: 160_000])
    evaluate_stn_list_response(resp.status_code, resp.body, date, base_url)

  end

  def evaluate_stn_list_response(200, body, _date, _base_url ) do
    body
    |> Floki.find("a")
    |> Floki.attribute("href")
    |> Enum.drop(5)
    |> Enum.uniq()
    |> Enum.map(fn x -> String.replace(x, "/", "") end)
    |> MapSet.new()
  end

  def evaluate_stn_list_response(301, _body, date, base_url) do
    :timer.sleep(1500)
    get_stn_list_for_date(date, base_url)
  end

  def evaluate_stn_list_response(404, _body, date, base_url) do
    :timer.sleep(500)
    get_stn_list_for_date(date, base_url)
  end

  def evaluate_stn_list_response(code, _body, _date, _base_url) do
    {:error, "Failed to download #{code}"}
  end


  def develop_file_list_and_download(stn, date, base_url, dest_base) do
    xml_names = get_xml_names(stn, date, base_url, dest_base)

    params = Enum.zip(xml_names.full_name, xml_names.file_name)
    params
    |> IO.inspect()
    |> Enum.map(fn x -> download_file(x) end)

  end

  def get_xml_names(stn_num, date, base_url, dest_base) do
    daily_download_url = base_url <> date <> "/" <> stn_num <> "/"
    dest_dir = "#{dest_base}#{stn_num}/#{date}/"

    download_file_list(daily_download_url, dest_dir)
  end

  def download_file_list(url, dest) do
    resp = HTTPoison.get!(url, [], [timeout: 160_000, recv_timeout: 160_000])
    evaluate_file_list_response(resp.status_code, resp.body, url, dest)
  end

  def evaluate_file_list_response(200, body, url, dest) do
    file =
      body
      |> Floki.find("a")
      |> Floki.attribute("href")
      |> Enum.drop(5)

    full_url =
      file
      |> Enum.map(fn x -> url <> x end)

    file_name =
      file
      |> Enum.map(fn x -> dest <> x end)

    %{file_name: file_name, full_name: full_url}
  end

  def evaluate_file_list_response(404, _body, url, dest) do
    :timer.sleep(500)
    download_file_list(url, dest)
  end

  def evaluate_file_list_response(_, _body, _url, dest) do
    {:error, "Failed to download #{dest}"}
  end

  def download_file({file_url, file_name}) do
    data = HTTPoison.get!(file_url, [], [timeout: 160_000, recv_timeout: 160_000])
    status = data.status_code
    evaluate_response(status, data, file_name, file_url)
  end

  def evaluate_response(200, data, name, _url) do
    File.write(name, data.body)
    {:ok, "file saved"}
  end

  def evaluate_response(404, _data, name, file) do
    :timer.sleep(500)
    download_file({file, name})
  end

  def evaluate_response(_404, _data, name, _file) do
   {:error, "file #{name} didn't work"}
  end

end
