class Array
  def fuzzy_filter(h)
    keep_if do |item|
      h.keys.each do |k|
        h[k] === item[k]
      end
    end
  end
end
