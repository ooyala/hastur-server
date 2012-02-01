class Array
  def fuzzy_filter(h)
    keep_if do |item|
      is_good = false
      h.keys.each do |k|
        is_good = true if h[k] === item[k]
      end
      is_good
    end
  end
end
