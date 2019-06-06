package com.scalar.jepsen.scalardl;

import com.scalar.ledger.asset.Asset;
import com.scalar.ledger.contract.Contract;
import com.scalar.ledger.exception.ContractContextException;
import com.scalar.ledger.ledger.Ledger;
import java.util.Optional;
import javax.json.Json;
import javax.json.JsonObject;

public class Read extends Contract {
  @Override
  public JsonObject invoke(Ledger ledger, JsonObject argument, Optional<JsonObject> property) {
    if (!argument.containsKey("key")) {
      throw new ContractContextException("required key 'key' is missing");
    }

    String key = String.valueOf(argument.getInt("key"));
    Optional<Asset> response = ledger.get(key);

    if (!response.isPresent()) {
      return null;
    }

    return Json
        .createObjectBuilder()
        .add("value", response.get().data().getInt("value"))
        .build();
  }
}
